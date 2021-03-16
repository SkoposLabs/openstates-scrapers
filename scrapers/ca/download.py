"""
This file defines functions for importing the CA database dumps in mysql.

The workflow is:
 - Drop & recreate the local capublic database.
 - Inspect the site with regex and determine which files have been updated, if any.
 - For each such file, unzip it & call import.
"""
import os
import re
import glob
import os.path
import subprocess
import logging
import lxml.html
import argparse
import sys
from datetime import datetime
from os.path import join, split
from functools import partial
from collections import namedtuple
import csv
import requests
import MySQLdb
from .skopos_mysql import get_mysql_passwords_from_file, get_aws_mysql_secret

# ----------------------------------------------------------------------------
# Logging config
logger = logging.getLogger("openstates.ca-update")
# logger.setLevel(logging.INFO)

# ch = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(message)s',
#                               datefmt='%H:%M:%S')
# ch.setFormatter(formatter)
# logger.addHandler(ch)

# ---------------------------------------------------------------------------
# Miscellaneous db admin commands.



# ---------------------------------------------------------------------------
# Connect to DB


#add this key to the docker-compose.yml if it is not there.
USE_AWS_KEY = os.environ.get("USE_AWS_KEY", "False")

if USE_AWS_KEY == "True":
    get_aws_mysql_secret()
    #do secret thing here:
else:
    password_list = get_mysql_passwords_from_file()
    skopos_user = password_list[1][0]
    skopos_password = password_list[1][1]
    #either get the environment variable, or use the default...
    #use the default if this is the first time you're ever running docker.
    MYSQL_HOST = os.environ.get("MYSQL_HOST", "localhost")
    MYSQL_USER = os.environ.get("MYSQL_USER", skopos_user)
    MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", skopos_password)


BASE_URL = "https://downloads.leginfo.legislature.ca.gov/"


def clean_text(s):
    # replace smart quote characters
    s = re.sub(r"[\u2018\u2019]", "'", s)
    s = re.sub(r"[\u201C\u201D]", '"', s)
    s = s.replace("\xe2\u20ac\u02dc", "'")
    return s
    


#2021-03-14 skopos function to initialize the skopos passwords. 
#you only need to run this once, with the flag --init_db_psword
def intialize_db_passwords():

    if USE_AWS_KEY == "True":
        print("Running on AWS")
        #insert get secret code here...
    else:
        print("Running locally")
        password_list = get_mysql_passwords_from_file()
        root_password = password_list[0][1]
        skopos_user = password_list[1][0]
        skopos_password = password_list[1][1]

        print("trying to connect to capublic with root no password...")
        try:
            connection = MySQLdb.connect(
                host=MYSQL_HOST, user="root", passwd=root_password, db="information_schema"
            )
            
            connection.autocommit(True)
            cursor = connection.cursor()

            cursor.execute('SHOW DATABASES where `database` like "%information_schema%";')
            m = cursor.fetchone()
            print("The first database is ",m[0])
            
            if m[0].strip() == "information_schema":
                print("Connected as root no password")
            
            '''
            set_root_psword = "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('" + root_password + "');"
            
            cursor.execute(set_root_psword)
            cursor.execute("flush privileges;")
            
            #this is original. keep this if you need to go back...
            #GRANT ALL PRIVILEGES ON *.* TO `root`@`%` WITH GRANT OPTION;
            
            cursor.execute('show grants;')
            for row in cursor:
                print(row)
                
            #force a password on root.
            update_root_grant = "GRANT ALL PRIVILEGES ON *.* TO `root`@`%` IDENTIFIED BY '" + root_password + "';"
            
            cursor.execute(update_root_grant)
            cursor.execute("flush privileges;")
            '''

            create_skopos_user = "CREATE USER '" + skopos_user + "'@`mysql` IDENTIFIED BY '" + skopos_password + "';"
            print(create_skopos_user)

            cursor.execute(create_skopos_user)
            cursor.execute("flush privileges;")

            update_skopos_grant = "GRANT ALL PRIVILEGES ON *.* TO '" + skopos_user + "'@`mysql`;"
            print(update_skopos_grant)
            
            cursor.execute(update_skopos_grant)
            cursor.execute("flush privileges;")
            
            connection.close()

        except BaseException as e: 
            exc_type, exc_value, exc_traceback = sys.exc_info()
            error_message = str(exc_type) + " " + str(exc_value) + " on line "+ str(exc_traceback.tb_lineno)
            print(error_message)
            contents = "ERROR " + error_message


def db_drop():
    """Drop the database."""
    logger.info("dropping capublic...")

    try:
        connection = MySQLdb.connect(
            host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD, db="capublic"
        )
    except MySQLdb._exceptions.OperationalError:
        # The database doesn't exist.
        logger.info("...no such database. Bailing.")
        return

    connection.autocommit(True)
    cursor = connection.cursor()

    cursor.execute("DROP DATABASE IF EXISTS capublic;")

    connection.close()
    logger.info("...done.")


# ---------------------------------------------------------------------------
# Functions for updating the data.
DatRow = namedtuple(
    "DatRow",
    [
        "bill_version_id",
        "bill_id",
        "version_num",
        "bill_version_action_date",
        "bill_version_action",
        "request_num",
        "subject",
        "vote_required",
        "appropriation",
        "fiscal_committee",
        "local_program",
        "substantive_changes",
        "urgency",
        "taxlevy",
        "bill_xml",
        "active_flg",
        "trans_uid",
        "trans_update",
    ],
)


def dat_row_2_tuple(row):
    """Convert a row in the bill_version_tbl.dat file into a
    namedtuple.
    """
    cells = row.split("\t")
    res = []
    for cell in cells:
        if cell.startswith("`") and cell.endswith("`"):
            res.append(cell[1:-1])
        elif cell == "NULL":
            res.append(None)
        else:
            res.append(cell)
    return DatRow(*res)


def encode_or_none(value):
    return value.encode() if value else None


def load_bill_versions(connection):
    """
    Given a data folder, read its BILL_VERSION_TBL.dat file in python,
    construct individual REPLACE statements and execute them one at
    a time. This method is slower that letting mysql do the import,
    but doesn't fail mysteriously.
    """

    sql = """
        REPLACE INTO capublic.bill_version_tbl (
            BILL_VERSION_ID,
            BILL_ID,
            VERSION_NUM,
            BILL_VERSION_ACTION_DATE,
            BILL_VERSION_ACTION,
            REQUEST_NUM,
            SUBJECT,
            VOTE_REQUIRED,
            APPROPRIATION,
            FISCAL_COMMITTEE,
            LOCAL_PROGRAM,
            SUBSTANTIVE_CHANGES,
            URGENCY,
            TAXLEVY,
            BILL_XML,
            ACTIVE_FLG,
            TRANS_UID,
            TRANS_UPDATE)

        VALUES (%s)
        """
    sql = sql % ", ".join(["%s"] * 18)

    cursor = connection.cursor()
    with open("BILL_VERSION_TBL.dat") as f:
        for row in f:
            # The files are supposedly already in utf-8, but with
            # copious bogus characters.
            row = clean_text(row)
            row = dat_row_2_tuple(row)
            with open(row.bill_xml) as f:
                text = f.read()
                text = clean_text(text)
                row = row._replace(bill_xml=text)
                cursor.execute(sql, [encode_or_none(column) for column in row])

    cursor.close()


def load(folder, sql_name=partial(re.compile(r"\.dat$").sub, ".sql")):
    """
    Import into mysql any .dat files located in `folder`.

    First get a list of filenames like *.dat, then for each, execute
    the corresponding .sql file after swapping out windows paths for
    `folder`.

    This function doesn't bother to delete the imported data files
    afterwards; they'll be overwritten within a week, and leaving them
    around makes testing easier (they're huge).
    """

    logger.info("Loading data from %s..." % folder)
    os.chdir(folder)

    connection = MySQLdb.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        passwd=MYSQL_PASSWORD,
        db="capublic",
        local_infile=1,
    )
    connection.autocommit(True)

    filenames = glob.glob("*.dat")

    for filename in filenames:

        # The corresponding sql file is in data/ca/dbadmin
        _, filename = split(filename)
        sql_filename = join("../pubinfo_load", sql_name(filename).lower())
        with open(sql_filename) as f:

            # Swap out windows paths.
            script = f.read().replace(r"c:\\pubinfo\\", folder)

        _, sql_filename = split(sql_filename)
        logger.info("loading " + sql_filename)
        if sql_filename == "bill_version_tbl.sql":
            logger.info("inserting xml files (slow)")
            load_bill_versions(connection)
        else:
            cursor = connection.cursor()
            cursor.execute(script)
            cursor.close()

    connection.close()
    os.chdir("..")
    logging.info("...Done loading from %s" % folder)


def db_create():
    """Create the database"""

    logger.info("Creating capublic...")

    dirname = get_zip("pubinfo_load.zip")
    os.chdir(dirname)

    with open("capublic.sql") as f:
        # Note: apparently MySQLdb can't execute compound SQL statements,
        # so we have to split them up.
        sql_statements = f.read().split(";")

    connection = MySQLdb.connect(
        host=MYSQL_HOST, user=MYSQL_USER, passwd=MYSQL_PASSWORD
    )
    print(
        f"mysql connection host={MYSQL_HOST}, user={MYSQL_USER}, password={MYSQL_PASSWORD}"
    )
    connection.autocommit(True)
    cursor = connection.cursor()

    # MySQL warns in OCD fashion when executing statements relating to
    # a table that doesn't exist yet. Shush, mysql...
    import warnings

    warnings.filterwarnings("ignore", "Unknown table.*")

    for sql in sql_statements:
        cursor.execute(sql)

    cursor.close()
    connection.close()
    os.chdir("..")


def get_contents():
    resp = {}
    html = requests.get(BASE_URL).text
    doc = lxml.html.fromstring(html)
    # doc.make_links_absolute(BASE_URL)
    rows = doc.xpath("//table/tr")
    for row in rows[2:]:
        date = row.xpath("string(td[3])").strip()
        if date:
            date = datetime.strptime(date, "%d-%b-%Y %H:%M")
            filename = row.xpath("string(td[2]/a[1]/@href)")
            resp[filename] = date
    return resp


def _check_call(*args):
    logging.info("calling " + " ".join(args))
    subprocess.check_call(args)


def get_zip(filename):
    dirname = filename.replace(".zip", "")
    _check_call("wget", "--no-check-certificate", BASE_URL + filename)
    _check_call("rm", "-rf", dirname)
    _check_call("unzip", filename, "-d", dirname)
    _check_call("rm", "-rf", filename)
    return dirname


def get_data(contents, year):
    newest_file = "2000"
    newest_file_date = datetime(2000, 1, 1)
    files_to_get = []

    if year:
        files_to_get.append(f"pubinfo_{year}.zip")
    else:
        # get file for latest date
        for filename, date in contents.items():
            date_part = filename.replace("pubinfo_", "").replace(".zip", "")
            if date_part.startswith("daily") and date > newest_file_date:
                newest_file = filename
                newest_file_date = date
        files_to_get.append(newest_file)

    for file in files_to_get:
        dirname = get_zip(file)
        load(dirname)


#note to skopos developer: don't use this script to upload archived CA sessions.
#use the code in stateleg to handle archived sessions.
if __name__ == "__main__":
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument("--year", action="store", type=int)
    my_parser.add_argument("--init_db_psword", action="store", type=bool, default=False)
    args = my_parser.parse_args()
    year = args.year
    init_db_psword = args.init_db_psword

    print("The year is " + str(year))
    print("The init_db_psword is " + str(init_db_psword))
    
    #skopos code to create passwords...
    if init_db_psword:
        intialize_db_passwords()
    else:
        db_drop()
        db_create()
        contents = get_contents()
        get_data(contents, year)

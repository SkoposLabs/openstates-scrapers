import csv
import os

#use in your local instance.
def get_mysql_passwords_from_file():
    root_dir = getRootProjectDir()
    password_file = root_dir + "passwords/mysql.csv"
    with open(password_file, 'rt') as csvfile:
        auth = csv.reader(csvfile, delimiter=',', quotechar='|')
        password_list = list(auth)
    return password_list

def get_aws_mysql_secret():
    pass

def getRootProjectDir():
    root_dir = os.path.abspath(os.path.join(os.path.dirname( __file__ ), '../..')) + "/"
    return root_dir
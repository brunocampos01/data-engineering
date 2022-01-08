import configparser
import logging
import os
from pathlib import Path

import mysql.connector

__version__ = '0.1'
__author__ = 'Bruno Campos'

path_project = str(Path(__file__).parent)
path_config = '/config.cfg'

config = configparser.ConfigParser()
config.read(os.path.join(path_project + path_config))

user_creator = config['mysql']['user_creator']
password_creator = config['mysql']['password_creator']
mysql_user = config['mysql']['user']
mysql_password = config['mysql']['password']
mysql_hostname = config['mysql']['hostname']
mysql_port = config['mysql']['port']
database_name = config['mysql']['database_name']

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s: %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

create_database_sample = """CREATE TABLE IF NOT EXISTS `modules`
                            (
                                `cod_module`     int         NOT NULL AUTO_INCREMENT,
                                `execution_date` date        NOT NULL,
                                `module_name`    varchar(50) NOT NULL,
                                `module_status`  boolean     NOT NULL,
                                `start_time`     datetime    NOT NULL,
                                `end_time`       datetime    NOT NULL,
                                `execution_time` time        NOT NULL,
                                CONSTRAINT
                                    `PK_MODULE` PRIMARY KEY (`cod_module`)
                            ) ENGINE = MyISAM
                              DEFAULT CHARSET = UTF8;"""


class MySQLHelper(object):

    def __init__(self, hostname: str, port: str, user: str,
                 password: str):
        self.__conn = self.__connect(host=hostname,
                                     port_db=port,
                                     user_name=user,
                                     passwd=password)
        self.__cursor = self.__conn.cursor(buffered=True)

    def __connect(self, host: str, port_db: str, user_name: str, passwd: str):
        """
        :arg:
        configparser --> config.cfg

        :return:
        Object connection
        """
        connection = mysql.connector.connect(host=host,
                                             port=port_db,
                                             user=user_name,
                                             password=passwd)
        connection.autocommit = True
        connection.get_warnings = True

        return connection

    def __del__(self):
        self.__conn.close()
        self.__cursor.close()

    def create_user(self, user: str, password: str):
        """
        :param user: master user
        :param password: master user
        :return: reference of object which it was called
        """
        try:
            self.__cursor.execute(
                    f"CREATE USER IF NOT EXISTS "
                    f"'{user}'@'%' IDENTIFIED BY '{password}';")
            return self
        except mysql.connector.Error as err:
            logging.error(
                    f"Failed creating user:"
                    f" {err}, user: {user}, password: {password}")
            raise

    def grant_privileges(self, user: str, database: str):
        """
        :param user: name new user
        :param database: db which new user can access
        :return: reference of object which it was called
        """
        try:
            self.__cursor.execute(
                    f"GRANT ALL PRIVILEGES ON {database}.* TO '{user}'@'%';")
            self.__cursor.execute("FLUSH PRIVILEGES;")
            return self
        except mysql.connector.Error as err:
            logging.error(
                    f"Failed execute query:"
                    f" {err}, user: {user}, database: {database}")
            raise

    def create_database(self, name_db: str):
        try:
            self.__cursor.execute(
                    'CREATE DATABASE IF NOT EXISTS {};'.format(name_db))
            return self
        except mysql.connector.Error as err:
            logging.error(
                    f"Failed create database:"
                    f" {err}, name: {name_db}")
            raise

    def with_database(self, name: str):
        """
        :param name: database name
        :return: reference of object which it was called
        """
        try:
            self.__cursor.execute("USE %s;" % name)
            return self
        except mysql.connector.Error as err:
            logging.error(
                    f"Failed execute query: {err}, name: {name}")
            raise

    def execute_statement(self, statement: str):
        """
        :return: reference of object which it was called
        """
        try:
            self.__cursor.execute(statement)
            return self
        except mysql.connector.Error as err:
            logging.error(
                    f"Failed execute query: {err}, query: {statement}")
            raise


def main():
    # Create user
    MySQLHelper(hostname=mysql_hostname,
                port=mysql_port,
                user=user_creator,
                password=password_creator) \
        .create_user(user=mysql_user, password=mysql_password) \
        .grant_privileges(user=mysql_user,
                          database=database_name)  # not necessary db exists to grant privileges

    # Create database and tables
    MySQLHelper(hostname=mysql_hostname,
                port=mysql_port,
                user=mysql_user,
                password=mysql_password) \
        .create_database(name_db=database_name) \
        .with_database(name=database_name) \
        .execute_statement(statement=create_database_sample)


if __name__ == "__main__":
    """
    Running this file to create USER, DATABASE and TABLES
    """
    main()

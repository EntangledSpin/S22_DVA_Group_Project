import os.path
import psycopg2
import time
import datetime
import pandas as pd
import sqlite3
import os
import sqlalchemy
from sqlalchemy import orm
import sys
from podcast_discovery.config import DB_PORT,DB_USER,DB_USER_PW,DB_NAME,DB_HOST

RETRIES = 3
CHUNKSIZE = 10000





DB_CONN_STRING = (
    f"host='{DB_HOST}' dbname='{DB_NAME}' user='{DB_USER}' password='{DB_USER_PW}'"
)


class Database:
    """
    Database is a helper class that we can call that has an engine and a session object
    associated with it.  This means we can do things like:

    db = Database()
    db.create_connection()
    result = db.session.execute('select 1;')
    etc.
    """

    def __init__(
        self,
        dbuser=DB_USER,
        dbpassword=DB_USER_PW,
        dbhost=DB_HOST,
        dbdatabase_port=DB_PORT,
        dbdatabase_name=DB_NAME,
    ):
        self.host = dbhost
        self.conn_str = "postgresql+psycopg2://{user}:{password}@{database_ip}:{database_port}/{database_name}".format(
            user=dbuser,
            password=dbpassword,
            database_ip=dbhost,
            database_port=dbdatabase_port,
            database_name=dbdatabase_name,
        )
        # print(self.conn_str)
        self.engine = None
        self.session = None
        self.create_connection()
        self.test_connection()
        self.user_list = ["postgres"]

    def create_connection(self, stream=False):
        """
        Attaches the engine and a session attribute of the engine to the Database class.

        :return: None
        """
        self.engine = sqlalchemy.create_engine(self.conn_str)
        session_maker = orm.sessionmaker(autoflush=True)
        session_maker.configure(bind=self.engine)
        self.session = session_maker()

    def create_psycopg2_connection(self):
        self.conn = psycopg2.connect(self.conn_str)

    def create_psycopg2_cursor(self, itersize=False):
        self.create_psycopg2_connection()
        self.cur = self.conn.cursor()

        if itersize:
            self.cur.itersize = itersize

    def close_connection(self):
        self.engine.dispose()

    def reconnect(self):
        try:
            self.close_connection()
        except:
            pass

        self.create_connection()

    def create_database(self):
        """
        When you first access a SQLite database, if it doesn't exist yet, the file won't get
        populated. This simply creates a test table and then drops that test table so that there is
        a file where we intend on the file existing.

        :return:
        """
        self.create_table()
        self.tear_down_table()

    def create_table(self):
        """
        Create a test table and insert the number 1 into an unnamed column as row 1.

        :return:
        """
        try:
            self.session.execute("CREATE TABLE test as select 1;")
            self.session.commit()
        except sqlalchemy.exc.OperationalError:
            self.session.rollback()




    def tear_down_table(self):
        """
        Drop the test table.

        :return:
        """
        try:
            self.session.execute("DROP TABLE test;")
            self.session.commit()
        except sqlalchemy.exc.OperationalError:
            self.session.rollback()

    def test_connection(self):
        sql = "select 1"
        result = self.session.execute(sql).fetchone()[0]
        if result == 1:
            print(f"Database Connected Successfully... {self.host}")

    def read_sql_path(self, sql_path):
        with open(sql_path, "r") as f:
            return f.read().strip()

    def get_session(self):

        session_maker = orm.sessionmaker(autoflush=True)
        session_maker.configure(bind=self.engine)
        return session_maker()

    def execute_sql(
        self,
        sql = None,
        sql_path = None,
        commit=False,

        return_list=False,
        return_dict=False,
        debug=False,
    ):
        session = self.get_session()
        if sql_path is not None:
            sql = self.read_sql_path(sql_path)
        else:
            sql = sql

        result =  session.execute(sql)

        if commit:
                try:
                    session.commit()

                except:
                    session.rollback()

                finally:

                    session.close()

        if debug:
            print(sql)

        if return_list:
            items = []
            for item in result:
                items.append(item[0])

            return items

        elif return_dict:
                return result.mappings().all()



    def setup_schema(self, schema_list):
        for schema in schema_list:
            sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
            self.session.execute(sql)
            self.session.commit()

    def set_schema_permissions(self, schema_list, print_statements=False):
        for schema in schema_list:
            for user in self.user_list:
                sql = f'grant usage on schema {schema} to "{user}";'
                if print_statements:
                    print(sql)
                self.session.execute(sql)
                self.session.commit()

        for schema in schema_list:
            for user in self.user_list:
                sql = f"""
                select table_name from information_schema.tables where table_schema = '{schema}'  
                UNION
select table_name from information_schema.views where table_schema = '{schema}' order by table_name;

                """
                result = [a[0] for a in self.session.execute(sql)]
                for table in result:
                    sql = f'grant select on "{schema}"."{table}" to "{user}";'
                    if print_statements:
                        print(sql)
                    self.session.execute(sql)
                    self.session.commit()
        self.session.commit()


if __name__ == "__main__":
    d = Database()
    print(d)

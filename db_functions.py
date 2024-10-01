# -*- coding: utf-8 -*-
"""
Python 3.7.4 

"""

from unicodedata import name

# import yaml
import psycopg2
import json
import os
import pandas as pd
from io import StringIO
import csv
import re
from datetime import datetime

# import time
from sqlalchemy import create_engine

DB = "pg"

##############################################################


def get_ddl_credentials():
    # Get credentials from CICD variables
    if not os.environ.get("DB_USER") is None:
        ddl_user_name = os.environ.get("DB_USER")
        ddl_pwd = os.environ.get("DB_PW")
        ddl_host = os.environ.get("DB_HOST")
        ddl_port = os.environ.get("DB_PORT")
        ddl_dbname = os.environ.get("DB_NAME")
    # Get credentials from local ddl.json file if there are no CICD variables
    else:
        with open("ddl.json") as json_file:
            data = json.load(json_file)
            ddl_user_name = data["DB_USER"]
            ddl_pwd = data["DB_PW"]
            ddl_host = data["DB_HOST"]
            ddl_port = data["DB_PORT"]
            ddl_dbname = data["DB_NAME"]

    return ddl_user_name, ddl_pwd, ddl_host, ddl_port, ddl_dbname


##############################################################


def decorator_timer(fun):
    def wrapper(*args):
        print(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            " - function",
            fun.__name__,
            "started",
        )
        x = fun(*args)
        print(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            " - function",
            fun.__name__,
            "completed",
        )
        return x

    return wrapper


##############################################################


def test_connection(connection):

    cursor = connection.cursor()
    cursor.execute("SELECT '123' AS test")
    rows = cursor.fetchall()

    return rows


##############################################################


def db_connection_engine(source):
    # create connection engine to a specified source ('atl'/DB)

    try:

        if source.lower() == DB:

            ddl_user_name, ddl_pwd, ddl_host, ddl_port, ddl_dbname = (
                get_ddl_credentials()
            )

            # create conneciton string
            conn_string = (
                ddl_user_name
                + ":"
                + ddl_pwd
                + "@"
                + ddl_host
                + ":"
                + ddl_port
                + "/"
                + ddl_dbname
            )

            # create connection engine
            return create_engine(f"postgresql+psycopg2://{conn_string}")

        else:
            print("datasource not recognized")
            return None

    except Exception as e:
        print("ERROR: unable to connect to DDL")
        print(e)

        return None


##############################################################


def db_connector(source):
    # establishes connection to a specified source ('atl'/DB)

    try:

        if source.lower() == DB:

            ddl_user_name, ddl_pwd, ddl_host, ddl_port, ddl_dbname = (
                get_ddl_credentials()
            )

            # establish ddl connection
            conn = psycopg2.connect(
                host=ddl_host,
                dbname=ddl_dbname,
                user=ddl_user_name,
                password=ddl_pwd,
                port=ddl_port,
            )

        else:
            print("datasource not recognized")
            return None

        return conn

    except Exception as e:
        print("ERROR: unable to connect to DDL")
        print(e)

        return None


# ---------------------------------------------------
# retrieve pandas DataFrame from a query
# ---------------------------------------------------
@decorator_timer
def get_query(source, query):
    # returns pandas DataFrame from query in the specified source ('atl'/DB)

    try:

        # check if query points to a file or sql string
        if query[-4:] == ".sql":

            file_name = os.path.join(*re.split(r"[\\/]", query))

            with open(file_name) as f:
                query = f.read()

        # check if query param points to object, create default SELECT from object
        elif re.search(r"^[A-Za-z0-9_]+(\.)?[A-Za-z0-9_]+$", query):
            query = "SELECT * FROM " + query

        engine = db_connection_engine(source)
        return pd.read_sql(query, engine)

    except Exception as e:
        print(e)

        return None


# ---------------------------------------------------
# execute SQL script
# ---------------------------------------------------
@decorator_timer
def execute_sql(source, query):
    # executes query within the specified data source ('atl'/DB)

    try:

        # check if query points to a file or sql string
        if query[-4:] == ".sql":

            file_name = os.path.join(*re.split(r"[\\/]", query))

            with open(file_name) as f:
                query = f.read()

        conn = db_connector(source)

        cursor = conn.cursor()
        cursor.execute(query)

        if source.lower() == DB:
            conn.commit()

        print(cursor.fetchall())

        cursor.close()
        conn.close()

    except Exception as e:
        print(e)

        return None


# ---------------------------------------------------
# upload dataframe to DB
# ---------------------------------------------------
# @decorator_timer
def upload_data(data, target_db, target_table, if_exists="fail", owner=""):

    try:
        if target_db.lower() == DB:

            # nested function for faster data upload
            def psql_insert_copy(table, conn, keys, data_iter):
                # gets a DBAPI connection that can provide a cursor
                dbapi_conn = conn.connection
                with dbapi_conn.cursor() as cur:
                    s_buf = StringIO()
                    writer = csv.writer(s_buf)
                    writer.writerows(data_iter)
                    s_buf.seek(0)

                    columns = ", ".join('"{}"'.format(k) for k in keys)
                    if table.schema:
                        table_name = "{}.{}".format(table.schema, table.name)
                    else:
                        table_name = table.name

                    sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
                    cur.copy_expert(sql=sql, file=s_buf)

            # function to create a db table

            def create_table_ddl(df, table):

                # dictionary of pandas dtype to SQL data types
                datatype_mapping = {
                    "float64": "DOUBLE PRECISION",
                    "int64": "BIGINT",
                    "int32": "INT",
                    "object": "TEXT",
                    "datetime64[ns]": "TEXT",
                    "bool": "BOOLEAN",
                }

                # extract dataframe column names + types
                df_columns = df.dtypes.astype(str).to_dict()

                # translate pandas data types to sql data types
                # for unidentified types use text
                sql_column_types = {}
                for key, value in df_columns.items():
                    sql_column_types[key] = (
                        datatype_mapping[value]
                        if value in datatype_mapping.keys()
                        else "TEXT"
                    )

                # join column definitions into SQL statement
                sql_columns_statement = ",\n".join(
                    [f"{col} {col_type}" for col, col_type in sql_column_types.items()]
                )

                # generate table definition script
                table_definition = f"""
                DROP TABLE IF EXISTS {table};
                CREATE TABLE {table} (
                {sql_columns_statement}
                ) DISTRIBUTED RANDOMLY;
                ALTER TABLE {table} OWNER TO {owner};
                """

                # create table in DDL
                print(f"Starting table creation '{table}'")
                execute_sql(DB, table_definition)
                print(f"table '{table}' succesfully created")

            # create connection engine
            engine = db_connection_engine(DB)

            # extract table schema and table name from input param
            target_split = target_table.split(".")
            tb_schema, tb_name = (
                ["public", target_split[0]]
                if len(target_split) == 1
                else [target_split[0], target_split[1]]
            )
            target_table = tb_schema + "." + tb_name

            # check if table exists
            print(f"Checking if table exists '{target_table}'")
            query = f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema || '.' || table_name = '{target_table}';"""
            table_exists = True if len(get_query(DB, query)) > 0 else False

            # convert all column names to lowercase
            data.columns = map(str.lower, data.columns)

            # table already exist
            if (table_exists == False) or (
                table_exists == True and if_exists == "replace"
            ):

                # create table
                create_table_ddl(data, target_table)

                # append data
                data.to_sql(
                    name=tb_name,
                    con=engine,
                    schema=tb_schema,
                    method=psql_insert_copy,
                    if_exists="append",
                    index=False,
                )

            elif table_exists == True and if_exists == "fail":

                raise Exception(
                    f"table '{target_table}' already exists -> cancelling data upload"
                )

            elif table_exists == True and if_exists == "append":

                # append data
                data.to_sql(
                    name=tb_name,
                    con=engine,
                    schema=tb_schema,
                    method=psql_insert_copy,
                    if_exists="append",
                    index=False,
                )

            print("data uploaded successfully")
            return None

        else:
            print("datasource not recognized")
            return None

    except Exception as e:
        print(e)
        return None


##############################################################


if __name__ == "__main__":
    print(os.environ.get("DB_USER"))
    print(f"DDL: {test_connection(db_connector(DB))}")
    # print(f"DDL: {get_query(DB, 'ws_mkt_dst.tb_b2b_mf_gbl_configuration_check')}")
    # print(f"DDL: {get_query(DB, 'SELECT * FROM ws_mkt_dst.tb_b2b_mf_gbl_configuration_check LIMIT 10')}")

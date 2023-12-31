import pandas as pd
from config import url
from pyspark.sql import SparkSession
import os
import pydantic as pyd
from pyspark.sql import DataFrame
import importlib
from pyspark.sql.functions import col, count
import sqlite3
from sqlite3 import Connection
from functools import wraps
from datetime import date
from pyspark.conf import SparkConf

spark_home = os.environ.get("SPARK_HOME")
driver_jar = "mssql-jdbc-12.2.0.jre8.jar"
driver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), driver_jar)
spark_conf = SparkConf()
spark_conf.set("spark.jars", driver_path)

TABLE_NAMES = sorted(
    ["Brands", "Categories", "Customers", "Order_items", "Orders", "Products", "Staffs", "Stocks", "Stores"])
PRIMARY_KEYS = sorted(
    ["brand_id", "category_id", "customer_id", "item_id", "order_id", "product_id", "staff_id", "stock_id", "store_id"])

db_file = 'copydb.db'


def get_credentials():
    with open('env.txt', 'r') as env_file:
        for line in env_file:
            key, value = line.strip().split('=', 1)
            os.environ[key] = value
    properties = {
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD")

    }
    return properties


def get_schema(file_path: str = "tableschemas.py") -> list:
    """Imports BaseModel objects from file with tables' models
    :param file_path: filepath string
    :return: list of objects
    """
    objects = []
    module_name = file_path.replace(".py", "")
    module = importlib.import_module(module_name)
    for class_name in dir(module):
        obj = getattr(module, class_name)
        if isinstance(obj, type):
            objects.append(obj)

    return objects


def zip_setup() -> list:
    """ combines required lists
    :return: list
    """
    return list(zip(TABLE_NAMES, get_schema(), PRIMARY_KEYS))


def report_writer_decorator(output_file: str = f"report{date.today().strftime('%Y-%m-%d')}.txt"):
    """ writes checks messages to report file
    :param output_file: filename
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            message = func(*args, **kwargs)
            with open(output_file, "a") as file:
                if message:
                    file.write(message)
                    file.write("\n")
            return message

        return wrapper

    return decorator


@report_writer_decorator()
def validate(df: DataFrame, model: pyd.BaseModel, index_offset: int = 2) -> str:
    """ Validates dataframes with tables' models

    :param df: spark DataFrame
    :param model: pydantic BaseModel
    :param index_offset:
    :return: count of good_data rows, bad_data rows and bad_data rows details
    """
    good_data = []
    bad_data = []
    df_rows = [row.asDict() for row in df.collect()]
    for index, row in enumerate(df_rows):
        try:
            model(**row)
            good_data.append(row)
        except pyd.ValidationError as e:
            row['Errors'] = [error_message['msg'] for error_message in e.errors()]
            row['Error_row_num'] = index + index_offset
            bad_data.append(row)
    if bad_data:
        return f"Validation failed on row count: {len(bad_data)}"


@report_writer_decorator()
def check_uniqueness(df: DataFrame, tablename: str) -> str:
    """ Checks uniqueness of rows in dataframe
    :param tablename: string of table name
    :param df: DataFrame
    :return: message sting with test result
    """
    total_count = df.count()
    distinct_count = df.distinct().count()

    if total_count != distinct_count:
        return f"{tablename}: Duplicate rows found."


@report_writer_decorator()
def check_foreign(df: DataFrame, df2: DataFrame, primary: str, table1: str, table2: str) -> str:
    """ Checks if values in 2nd table foreign key column excist in
    table that is being checked primary key column values

    :param df: Dataframe
    :param df2: DataFrame
    :param primary: column name
    :return: message string
    """
    if primary in df2.columns:
        primary_values = [row[0] for row in df.select(primary).collect()]
        if df2.filter(col(primary).isin(primary_values)).count() <= 0:
            return f"{table1} vs {table2} : keys connection error found"


@report_writer_decorator()
def check_nulls(df: DataFrame, tablename: str) -> str:
    """ Checking how much null values is in table column

    :param tablename: table name
    :param df: DataFrame
    :return:
    """
    message = []
    for column in df.columns:
        checked_df = df.select(col(column).isNull().alias('isNull')).groupBy('isNull').count()
        null_count_df = checked_df.filter(col("isNull") == True)
        rows = null_count_df.collect()
        if rows:
            null_count = rows[0]['count']
            message.append(f"{column}: Null values {null_count}")
    if message:
        return f"Table: {tablename} Null values found \n column " + ''.join(message)


def write_to_file(df: DataFrame, name: str) -> None:
    """ Converts dataframe to pandas and writes to Excel file

    :param df: Dataframe
    :param name: filename
    :return:
    """
    pd_df = df.toPandas()
    pd_df.to_excel(name, index=False)


def write_to_db(df: DataFrame, table_name: str, conn: Connection) -> None:
    """ Converts datframe to pandas and writes to database

    :param df: datafrmae
    :param db_file: path to database
    :param table_name: name of the table
    :return:
    """

    pd_df = df.toPandas()
    bad_columns = pd_df.select_dtypes(include='object').columns
    pd_df[bad_columns] = pd_df[bad_columns].astype('str')
    pd_df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.commit()


def pipeline(table: list, properties):
    """All operations combined

    :param table: list of tuples with table name, table BaseModel and primary key column
    :return: None
    """

    df = spark.read.jdbc(url=url, table=table[0], properties=properties)
    # data quality checks
    check_uniqueness(df, table[0])
    validate(df, table[1])
    check_nulls(df, table[0])
    for table1 in setup_list:
        df1 = spark.read.jdbc(url=url, table=table1[0], properties=properties)
        check_foreign(df, df1, table[2], table[0], table1[0])

    # write to db
    write_to_db(df, table[0], conn)
    write_to_file(df, f"copied_data/{table[0]}.xlsx")
    print(f"{table[0]} is checked and copied")


def transform_copied_data(query: str) -> list:
    """ Runs query agains new database, fetches result

    :param query:
    :return: result of the query
    """
    conn = sqlite3.connect("copydb.db")
    cursor = conn.cursor()
    cursor.execute(query)
    tables = cursor.fetchall()

    return tables


if __name__ == "__main__":

    properties = get_credentials()
    setup_list = zip_setup()

    spark = SparkSession.builder \
        .appName("Read from Database") \
        .config(conf=spark_conf) \
        .getOrCreate()
    spark.conf.set("spark.sql.catalog.mssql", "com.microsoft.sqlserver.jdbc.SQLServerDialect")

    conn = sqlite3.connect(db_file)
    database_url = "jdbc:sqlite:copy.db"
    connection_properties = {
        "driver": "org.sqlite.JDBC",
        "url": database_url
    }
    cursor = conn.cursor()

    for table in setup_list:
        pipeline(table, properties)

        print("Copied data in DATABASE")
        cursor.execute(f"SELECT * FROM {table[0]} LIMIT 5")
        tables = cursor.fetchall()
        print(pd.DataFrame(tables))

    conn.close()

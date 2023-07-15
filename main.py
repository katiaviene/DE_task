import pandas as pd
from config import hadoop_home, spark_home, database, url, properties, jar
from pyspark.sql import SparkSession
import os
import pydantic as pyd
from pyspark.sql import DataFrame
import importlib
from pyspark.sql.functions import col, count
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table
from reportlab.lib import colors
import sqlite3
import openpyxl

os.environ["HADOOP_HOME"] = hadoop_home
os.environ["SPARK_HOME"] = spark_home
TABLE_NAMES = sorted(
    ["Brands", "Categories", "Customers", "Order_items", "Orders", "Products", "Staffs", "Stocks", "Stores"])
PRIMARY_KEYS = sorted(
    ["brand_id", "category_id", "customer_id", "item_id", "order_id", "product_id", "staff_id", "stock_id", "store_id"])

db_file = 'copydb.db'


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


def validate(df: DataFrame, model: pyd.BaseModel, index_offset: int = 2) -> tuple[int, int, list]:
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
    return len(good_data), len(bad_data), bad_data


def check_uniqueness(df: DataFrame) -> str:
    """ Checks uniqueness of rows in dataframe
    :param df: DataFrame
    :return: message sting with test result
    """
    total_count = df.count()
    distinct_count = df.distinct().count()

    if total_count == distinct_count:
        return "All rows are unique."
    else:
        return "Duplicate rows found."


def check_foreign(df, df2, primary):
    """ Checks if values in 2nd table foreign key column excist in
    table that is being checked primary key column values

    :param df: Dataframe
    :param df2: DataFrame
    :param primary: column name
    :return: message string
    """
    if primary in df2.columns:
        primary_values = [row[0] for row in df.select(primary).collect()]
        if df2.filter(col(primary).isin(primary_values)).count() > 0:
            return "good"
        else:
            return "not good"
    else:
        pass


def check_nulls(column: str):
    """ Checking how much null values is in table column

    :param column: column name
    :return:
    """
    return df.select(col(column).isNull().alias('isNull')).groupBy('isNull').count()


def create_pdf_report(output_file):
    """ Created pdf file with data quality check result

    :param output_file: filename
    :return: None
    """
    dataframe = pd.DataFrame({"id": [1, 2, 3], "name": [2, 3, 4]})
    data = [list(dataframe.columns)] + dataframe.values.tolist()
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    elements = []
    table = Table(data)
    style = [
        ('BACKGROUND', (0, 0), (-1, 0), colors.gray),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]
    table.setStyle(style)
    elements.append(table)
    doc.build(elements)


def write_to_file(df, name):
    """ Converts dataframe to pandas and writes to Excel file

    :param df: Dataframe
    :param name: filename
    :return:
    """
    pd_df = df.toPandas()
    pd_df.to_excel(name, index=False)


def write_to_db(df, db_file, table_name):
    """ Converts datframe to pandas and writes to database

    :param df: datafrmae
    :param db_file: path to database
    :param table_name: name of the table
    :return:
    """
    conn = sqlite3.connect(db_file)
    pd_df = df.toPandas()
    datetime_columns = pd_df.select_dtypes(include='datetime')
    pd_df[datetime_columns.columns] = datetime_columns.apply(
        lambda x: x.strftime("%Y-%m-%d") if x.notnull().all() else x)
    table_name = table_name
    try:
        pd_df.to_sql(table_name, conn, if_exists="replace", index=False)
    except:
        pass
    conn.commit()
    conn.close()


def transform_copied_data(query):
    """ Runs query agains new database, fetches result

    :param query:
    :return: result of the query
    """
    conn = sqlite3.connect("copydb.db")
    cursor = conn.cursor()
    cursor.execute(query)
    tables = cursor.fetchall()
    conn.close()
    return tables


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Read from Database") \
        .config("spark.driver.extraClassPath", jar) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    conn = sqlite3.connect(db_file)
    database_url = "jdbc:sqlite:copy.db"
    connection_properties = {
        "driver": "org.sqlite.JDBC",
        "url": database_url
    }

    info = list(zip(TABLE_NAMES, get_schema(), PRIMARY_KEYS))
    for table in info:
        df = spark.read.jdbc(url=url, table=table[0], properties=properties)
        unique_test = check_uniqueness(df)
        validate_test = validate(df, table[1])
        for table1 in info:
            df1 = spark.read.jdbc(url=url, table=table1[0], properties=properties)
            key_test = check_foreign(df, df1, table[2])
        for column in df.columns:
            null_test = check_nulls(column)
        write_to_db(df, db_file, table[0])
        write_to_file(df, f"copied_data/{table[0]}.xlsx")
    create_pdf_report('report.pdf')



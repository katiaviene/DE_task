import pandas as pd
from config import hadoop_home, spark_home, database, url, properties, jar
from pyspark.sql import SparkSession
import os
import pydantic as pyd
from pyspark.sql import DataFrame
import importlib
import inspect
from pyspark.sql.functions import col, count
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table
from reportlab.lib import colors

os.environ["HADOOP_HOME"] = hadoop_home
os.environ["SPARK_HOME"] = spark_home
TABLE_NAMES = ["Brands", "Categories", "Customers", "Order_items", "Orders", "Products", ]
PRIMARY_KEYS = ["brand_id", "category_id", "customer_id", "item_id", "order_id", "product_id"]




# table_names = ["Brands", "Categories", "Customers", "Order_items", "Orders", "Products", "Staffs", "Stocks", "Stores"]


def get_schema(file_path: str = "tableschemas.py") -> list:
    objects = []
    module_name = file_path.replace(".py", "")
    module = importlib.import_module(module_name)
    for class_name in dir(module):
        obj = getattr(module, class_name)
        if isinstance(obj, type):
            objects.append(obj)

    return objects


def validate(df: DataFrame, model: pyd.BaseModel, index_offset: int = 2) -> tuple[int, int, list]:
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
    return (len(good_data), len(bad_data), bad_data)


def check_uniqueness(df: DataFrame) -> str:
    total_count = df.count()
    distinct_count = df.distinct().count()

    if total_count == distinct_count:
        return "All rows are unique."
    else:
        return "Duplicate rows found."


def check_foreign(df, df2, primary):
    if primary in df2.columns:
        primary_values = [row[0] for row in df.select(primary).collect()]
        if df2.filter(col(primary).isin(primary_values)).count() > 0:
            return "good"
        else:
            return "not good"
    else:
        pass


def check_nulls(column):
    return df.select(col(column).isNull().alias('isNull')).groupBy('isNull').count()


def create_pdf_report(output_file):
    dataframe = pd.DataFrame({"id": [1, 2, 3], "name": [2, 3, 4]})
    # Convert DataFrame to a list of lists
    data = [list(dataframe.columns)] + dataframe.values.tolist()

    # Set up the PDF document
    doc = SimpleDocTemplate(output_file, pagesize=letter)
    elements = []

    # Create a table from the data
    table = Table(data)

    # Define table style
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

    # Add the table to the PDF document
    elements.append(table)

    # Build the PDF document
    doc.build(elements)


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Read from Database") \
        .config("spark.driver.extraClassPath", jar) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

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

    create_pdf_report('report.pdf')

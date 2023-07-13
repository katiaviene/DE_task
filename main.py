from pyspark.sql import SparkSession
import os
import pydantic as pyd
from pyspark.sql import DataFrame
import importlib

hadoop_home = "C:\\Users\\ekkor\\hadoop"  # Set the path to your Hadoop installation directory here
spark_home = 'C:\\Users\\ekkor\\spark\\spark-3.3.2-bin-hadoop3'
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["SPARK_HOME"] = spark_home

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read from Database") \
    .config("spark.driver.extraClassPath", "C:\\Users\\ekkor\\jdbc\\sqljdbc_12.2\\enu\\mssql-jdbc-12.2.0.jre8.jar") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# Configure the database connection properties
database = "BikeShop"

url = "jdbc:sqlserver://data-engineer-trial-db.cjuukq3gfs6h.eu-central-1.rds.amazonaws.com:1433;databaseName=" + database + ";encrypt=true;trustServerCertificate=true;"
properties = {
    "user": "de_candidate",
    "password": "b4rb0r4-d4t4"
}
# df = spark.read.jdbc(url=url, table="Categories", properties=properties)
# df.show()
# //////////////////////////////////////////////////////////////////////////////////////////////////////////////

def check_uniqueness(df):
    total_count = df.count()
    distinct_count = df.distinct().count()

    if total_count == distinct_count:
        return "All rows are unique."
    else:
        return "Duplicate rows found."


def validate(df: DataFrame, model: pyd.BaseModel, index_offset: int = 2) -> tuple[int, int]:
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
    return (len(good_data), len(bad_data))


def get_schema(file_path="tableschemas.py"):
    objects = []

    module_name = file_path.replace(".py", "")
    module = importlib.import_module(module_name)

    for class_name in dir(module):
        obj = getattr(module, class_name)
        if isinstance(obj, type):
            objects.append(obj)

    return objects


# table_names = ["Brands", "Categories", "Customers", "Order_items", "Orders", "Products", "Staffs", "Stocks", "Stores"]
table_names = ["Brands", "Categories"]
info = list(zip(table_names, get_schema()))

for table in info:
    df = spark.read.jdbc(url=url, table=table[0], properties=properties)
    print(check_uniqueness(df))
    print(validate(df, table[1]))

# Show the DataFrame

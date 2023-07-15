import pandas as pd
from pyspark.sql import SparkSession
from main import write_to_file, check_uniqueness
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
import os
import sys
import pytest

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local").appName("test").getOrCreate()


@pytest.mark.parametrize("data", [
    {
        "Name": ["John", "Bob", "Charlie"],
        "Age": [99, 30, 35]
    },
    {
        "Name": [None, "Emily", "Frank"],
        "Age": [40, 45, None]
    },

])
def test_write_to_file(tmpdir, data):
    schema = StructType([
        StructField("Name", StringType(), True, metadata={"nullable": True}),
        StructField("Age", IntegerType(), True, metadata={"nullable": True}),

    ])

    rows = list(zip(data["Name"], data["Age"], ))
    df = spark.createDataFrame(rows, schema)
    file_path = os.path.join(tmpdir, "test_output.xlsx")
    write_to_file(df, file_path)
    assert os.path.isfile(file_path)
    os.remove(file_path)

@pytest.mark.parametrize("data", [
    {
        "Name": [None, "Emily", "Frank"],
        "Age": [40, 45, None]
    },
    {
        "Name": [None, " ", "Frank"],
        "Age": [40, 45, None]
    },

])
def test_write_to_file_equal(tmpdir, data):
    schema = StructType([
        StructField("Name", StringType(), True, metadata={"nullable": True}),
        StructField("Age", IntegerType(), True, metadata={"nullable": True}),

    ])

    rows = list(zip(data["Name"], data["Age"], ))
    df = spark.createDataFrame(rows, schema)
    file_path = os.path.join(tmpdir, "test_output.xlsx")
    write_to_file(df, file_path)
    df_check = pd.read_excel(file_path)
    print(df_check)
    print(df.toPandas())
    assert df_check.equals(df.toPandas())
    os.remove(file_path)


@pytest.fixture
def test_data():
    data = [
        ("Alice", None),
        ("Bob", 30),
        ("Charlie", 35),
        ("Alice", None),
        ("Bob", 30)
    ]
    columns = ["Name", "Age"]
    return spark.createDataFrame(data, columns)


def test_check_uniqueness(test_data):
    result = check_uniqueness(test_data)
    assert result == "Duplicate rows found."
    unique_data = [
        ("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35)
    ]
    columns = ["Name", "Age"]
    unique_df = spark.createDataFrame(unique_data, columns)
    result = check_uniqueness(unique_df)
    assert result == "All rows are unique."

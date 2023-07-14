import pandas as pd
from pyspark.sql import SparkSession
from main import write_to_file
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
import os
import sys
import pytest


@pytest.mark.parametrize("data", [
    {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35]
    },
    {
        "Name": ["David", "Emily", "Frank"],
        "Age": [40, 45, 50]
    },
])
def test_write_to_file(tmpdir, data):
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder.master("local").appName("test").getOrCreate()
    data = {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35],

    }
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Age", IntegerType(), True),

    ])

    rows = list(zip(data["Name"], data["Age"], ))
    df = spark.createDataFrame(rows, schema)
    file_path = os.path.join(tmpdir, "test_output.xlsx")
    write_to_file(df, file_path)
    assert os.path.isfile(file_path)
    os.remove(file_path)

import pytest
from pyspark.sql import SparkSession, Row
import sys
import os

# Garante que o src está no path para importação
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
import data_quality  # Importa o módulo de qualidade de dados

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest").getOrCreate()
    yield spark
    spark.stop()

def test_null_check(spark):
    data = [Row(id=1, valor=100), Row(id=2, valor=None)]
    df = spark.createDataFrame(data)
    result = data_quality.check_nulls(df, ["valor"])
    assert result == {"valor": 1}

def test_negative_hours_check(spark):
    data = [Row(employee_id=1, hours=-5), Row(employee_id=2, hours=8)]
    df = spark.createDataFrame(data)
    result = data_quality.check_negative_hours(df, "hours")
    assert result == 1

def test_schema_validation(spark):
    df = spark.createDataFrame([Row(a=1, b="x")])
    expected_columns = ["a", "b"]
    assert data_quality.validate_schema(df, expected_columns)

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src import data_quality  

# Define uma fixture para criar uma SparkSession que será reutilizada nos testes
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("pytest").getOrCreate()

# Testa a função que verifica valores nulos em colunas específicas
def test_null_check(spark):
    data = [Row(id=1, valor=100), Row(id=2, valor=None)]  # Um valor nulo proposital
    df = spark.createDataFrame(data)                      # Cria DataFrame a partir dos dados
    result = data_quality.check_nulls(df, ["valor"])      # Executa a função a ser testada
    assert result == {"valor": 1}                         # Espera que haja 1 valor nulo na coluna "valor"

# Testa se a função identifica corretamente valores negativos em uma coluna numérica
def test_negative_hours_check(spark):
    data = [Row(employee_id=1, hours=-5), Row(employee_id=2, hours=8)]
    df = spark.createDataFrame(data)
    result = data_quality.check_negative_hours(df, "hours")
    assert result == 1  # Espera 1 valor negativo na coluna "hours"

# Testa se o schema do DataFrame corresponde ao esperado
def test_schema_validation(spark):
    df = spark.createDataFrame([Row(a=1, b="x")])            # Cria um DF com colunas a e b
    expected_columns = ["a", "b"]                            # Define o schema esperado
    assert data_quality.validate_schema(df, expected_columns)  # Valida se o schema bate

from pyspark.sql import SparkSession, functions as F
import sys

def check_nulls(df, columns):
    """Retorna um dicionário com a contagem de nulos por coluna."""
    nulls = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in columns
    ])
    counts = nulls.collect()[0].asDict()
    return counts

def check_negative_hours(df, column):
    """Retorna a contagem de valores negativos na coluna informada."""
    return df.filter(F.col(column) < 0).count()

def validate_schema(df, expected_columns):
    """Valida se o DataFrame possui exatamente as colunas esperadas."""
    return set(df.columns) == set(expected_columns)

# Opcional: função principal para rodar checks no arquivo Parquet
def run_checks():
    spark = SparkSession.builder.appName("data_quality").getOrCreate()
    df = spark.read.parquet("data/silver/points.parquet")

    # Exemplo de uso das funções
    null_counts = check_nulls(df, df.columns)
    print("Null counts por coluna:", null_counts)

    neg = check_negative_hours(df, "hours")
    print(f"Horas negativas encontradas: {neg}")

    # Exemplo de schema esperado
    expected = ["employee_id", "employee_name", "department", "vacation_start", "vacation_end", "absences", "status", "vacation_days", "hours"]
    schema_ok = validate_schema(df, expected)
    print(f"Schema válido: {schema_ok}")

    spark.stop()

if __name__ == "__main__":
    run_checks()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def process_raw_to_bronze():
    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()

    # Caminho absoluto para o CSV na camada raw
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_csv = os.path.join(base_dir, "data", "raw", "input_large.csv")
    output_parquet = os.path.join(base_dir, "data", "bronze")

    # Lê o CSV da camada raw
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_csv)

    # Tratamentos típicos para camada bronze
    df_bronze = (
        df.dropna(how="all")
          .withColumn("absences", col("absences").cast("int"))
          .withColumn("employee_id", col("employee_id").cast("int"))
          .fillna({"absences": 0})
          .dropDuplicates()
    )

    # Salva como Parquet diretamente na pasta bronze
    df_bronze.write.mode("overwrite").parquet(output_parquet)

    print(f"Dados bronze salvos em {output_parquet}")

    spark.stop()

if __name__ == "__main__":
    process_raw_to_bronze()
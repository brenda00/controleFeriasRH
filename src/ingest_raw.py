from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_raw_to_bronze():
    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()

    # Caminho do CSV gerado na camada raw
    input_csv = "../data/raw/input_large.csv"
    # Caminho para salvar os arquivos Parquet diretamente na bronze
    output_parquet = "../data/bronze"

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
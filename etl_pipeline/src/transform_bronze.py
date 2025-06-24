from pyspark.sql import SparkSession, functions as F

def transform_bronze():
    spark = SparkSession.builder.appName("transform_bronze").getOrCreate()

    # 1. Lê os dados do bronze (CSV)
    df = spark.read.csv("data/bronze/points.xlsx", header=True, inferSchema=True)

    # 2. Aplica limpeza básica:
    df_clean = (
        df.dropDuplicates()                                  # Remove linhas duplicadas
          .filter(F.col("employee_id").isNotNull())         # Remove registros sem ID
          .filter(F.col("hours").isNotNull())               # Remove registros sem horas
          .withColumn("hours", F.col("hours").cast("double")) # Converte hours para número
          .filter(F.col("hours") >= 0)                      # Remove horas negativas
    )

    # 3. Grava o resultado limpo como Parquet na pasta silver
    df_clean.write.mode("overwrite").parquet("data/silver/points.parquet")
    spark.stop()

if __name__ == "__main__":
    transform_bronze()

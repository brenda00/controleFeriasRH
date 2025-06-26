from pyspark.sql import SparkSession, functions as F
import os

def transform_bronze():
    spark = SparkSession.builder.appName("transform_bronze").getOrCreate()

    # Caminho absoluto para a pasta bronze
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bronze_path = os.path.join(base_dir, "data", "bronze")
    silver_path = os.path.join(base_dir, "data", "silver")  # <-- agora salva direto na pasta silver

    # 1. Lê os dados do bronze (Parquet)
    df = spark.read.parquet(bronze_path)

    # 2. Cria coluna de dias de férias e horas trabalhadas (exemplo: 8h/dia)
    df = (
        df.withColumn("vacation_start", F.to_date("vacation_start"))
          .withColumn("vacation_end", F.to_date("vacation_end"))
          .withColumn("vacation_days", F.datediff("vacation_end", "vacation_start") + F.lit(1))
          .withColumn("hours", (F.col("vacation_days") * 8) - (F.col("absences") * 8))
    )

    # 3. Limpeza e filtro
    df_clean = (
        df.dropDuplicates()
          .filter(F.col("employee_id").isNotNull())
          .filter(F.col("hours").isNotNull())
          .withColumn("hours", F.col("hours").cast("double"))
          .filter(F.col("hours") >= 0)
    )

    # 4. Grava como Parquet diretamente na pasta silver
    df_clean.write.mode("overwrite").parquet(silver_path)

    spark.stop()

if __name__ == "__main__":
    transform_bronze()

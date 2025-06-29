from pyspark.sql import SparkSession, functions as F
import os
import datetime

def transform_bronze():
    spark = SparkSession.builder \
        .appName("transform_bronze") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bronze_base_path = os.path.join(base_dir, "data", "bronze")
    hoje = datetime.datetime.today().strftime("%Y%m%d")
    silver_path = os.path.join(base_dir, "data", "silver", hoje)

    # Recupera a lista de partições disponíveis e pega a mais recente
    partitions = sorted(os.listdir(bronze_base_path), reverse=True)
    if not partitions:
        print(f" Nenhuma partição encontrada em {bronze_base_path}")
        return

    latest_partition = partitions[0]
    partition_path = os.path.join(bronze_base_path, latest_partition)
    print(f"Lendo dados da partição: {latest_partition}")

    # 1. Lê os dados da partição mais recente
    df = spark.read.parquet(partition_path)

    # 2. Cria coluna de dias de férias e horas trabalhadas
    df = (
        df.withColumn("vacation_start", F.to_date("vacation_start"))
          .withColumn("vacation_end", F.to_date("vacation_end"))
          .withColumn("vacation_days", F.datediff("vacation_end", "vacation_start") + F.lit(1))
          .withColumn("hours", (F.col("vacation_days") * 8) - (F.col("absences") * 8))
    )

    # 3. Limpeza e validação
    df_clean = (
        df.dropDuplicates()
          .filter(F.col("employee_id").isNotNull())
          .filter(F.col("hours").isNotNull())
          .withColumn("hours", F.col("hours").cast("double"))
          .filter(F.col("hours") >= 0)
          .withColumn("processing_date", F.lit(hoje))
    )

    # 4. Inclui a coluna com a data de processamento
    processing_date = datetime.datetime.today().strftime("%Y%m%d")
    df_final = df_clean.withColumn("processing_date", F.lit(processing_date))

    # 5. Grava como Parquet particionado pela data de processamento no formato desejado
    output_path = os.path.join(base_dir, "data", "silver", processing_date)
    df_final.write.mode("overwrite").parquet(output_path)

    print(f"Dados silver salvos em: {output_path}")
    spark.stop()

if __name__ == "__main__":
    transform_bronze()

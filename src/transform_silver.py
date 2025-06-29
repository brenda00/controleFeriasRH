from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, max as spark_max, min as spark_min
import os
import shutil
import glob
import datetime

def silver_to_gold():
    # Criação da sessão Spark com configurações de performance
    spark = (
        SparkSession.builder
        .appName("silver_to_gold")
        .config("spark.sql.shuffle.partitions", "4")  # Reduz número de partições geradas em operações de shuffle
        .config("spark.sql.adaptive.enabled", "true")  # Ativa execução adaptativa do Spark
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")  # Junta partições pequenas automaticamente
        .getOrCreate()
    )

    # Caminhos base
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    silver_base_path = os.path.join(base_dir, "data", "silver")
    gold_dir = os.path.join(base_dir, "data", "gold", "employee_hours_temp")
    gold_file = os.path.join(base_dir, "data", "gold", "employee_hours.csv")

    # Recupera a lista de partições disponíveis e pega a mais recente
    partitions = sorted(os.listdir(silver_base_path), reverse=True)
    if not partitions:
        print(f"Nenhuma partição encontrada em {silver_base_path}")
        return

    latest_partition = partitions[0]
    partition_path = os.path.join(silver_base_path, latest_partition)
    print(f"Lendo dados da partição silver mais recente: {latest_partition}")

    # Lê os dados da partição mais recente
    df_silver = spark.read.parquet(partition_path)

    # Agrupa os dados para gerar a camada gold com métricas por colaborador
    df_gold = (
        df_silver
        .groupBy("employee_id", "employee_name", "department")
        .agg(
            spark_sum("hours").alias("total_worked_hours"),
            spark_sum("absences").alias("total_absences"),
            avg("hours").alias("avg_hours_per_period"),
            spark_max("hours").alias("max_hours_in_period"),
            spark_min("hours").alias("min_hours_in_period")
        )
        .orderBy(col("total_worked_hours").desc())
    )

    # Salva como CSV único (coalesce(1)) temporariamente
    df_gold.coalesce(1).write.mode("overwrite").option("header", True).csv(gold_dir)

    # Move o CSV gerado para um nome fixo na pasta gold
    temp_csv = glob.glob(os.path.join(gold_dir, "*.csv"))[0]
    shutil.move(temp_csv, gold_file)
    shutil.rmtree(gold_dir)

    print(f"Gold salvo em: {gold_file}")
    spark.stop()

if __name__ == "__main__":
    silver_to_gold()

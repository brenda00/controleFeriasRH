from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, max as spark_max, min as spark_min
import os
import shutil
import glob

def silver_to_gold():
    spark = SparkSession.builder.appName("silver_to_gold").getOrCreate()

    # Caminho para ler os arquivos Parquet da silver
    silver_path = "../data/silver"
    # Caminho para salvar o relatório gold
    gold_dir = "../data/gold/employee_hours_temp"
    gold_file = "../data/gold/employee_hours.csv"

    # Lê os dados da silver
    df_silver = spark.read.parquet(silver_path)

    # Gold: calcula horas trabalhadas por colaborador e estatísticas por departamento
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

    # Salva o relatório gold como um único CSV temporário
    df_gold.coalesce(1).write.mode("overwrite").option("header", True).csv(gold_dir)

    # Move o arquivo CSV gerado para a raiz da pasta gold com nome fixo
    temp_csv = glob.glob(os.path.join(gold_dir, "*.csv"))[0]
    shutil.move(temp_csv, gold_file)
    shutil.rmtree(gold_dir)

    print(f"Gold salvo em {gold_file}")

    spark.stop()

if __name__ == "__main__":
    silver_to_gold()

from pyspark.sql import SparkSession, functions as F

def transform_silver():
    spark = SparkSession.builder.appName("transform_silver").getOrCreate()

    # 1. Lê dados limpos da pasta silver
    df = spark.read.parquet("data/silver/points.parquet")

    # 2. Agrega horas por colaborador
    df_ag = df.groupBy("employee_id").agg(
        F.sum("hours").alias("total_hours")
    )

    # 3. Grava o relatório Gold como CSV
    df_ag.write.mode("overwrite").csv("data/silver/points_report.csv", header=True)
    spark.stop()

if __name__ == "__main__":
    transform_silver()

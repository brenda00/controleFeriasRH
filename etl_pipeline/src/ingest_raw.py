from pyspark.sql import SparkSession

def ingest():
    # 1. Cria sessão Spark, necessária para usar DataFrames
    spark = SparkSession.builder.appName("ingest_raw").getOrCreate()

    dir_csv = "data/raw/input_large.csv"

    # 2. Lê arquivo Excel e converte em DataFrame
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(dir_csv) \


    # 3. Grava o DataFrame como CSV na pasta bronze
    df.write.mode("overwrite").csv("data/bronze/points.csv", header=True)

    # 4. Encerra a sessão Spark para liberar recursos
    spark.stop()

if __name__ == "__main__":
    ingest()
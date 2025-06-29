from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from datetime import datetime
import os

def process_raw_to_bronze():
    # Configurações avançadas de performance da sessão Spark
    spark = (
        SparkSession.builder
        .appName("bronze_ingest")
        # Memória alocada por executor
        .config("spark.executor.memory", "2g")
        # Memória adicional para uso fora do heap (UDFs, overhead nativo)
        .config("spark.executor.memoryOverhead", "512m")
        # Reduz número de partições após operações com shuffle (ex: joins)
        .config("spark.sql.shuffle.partitions", "4")
        # Define o paralelismo padrão para RDDs
        .config("spark.default.parallelism", "4")
        # Habilita execução adaptativa para otimização automática em tempo de execução
        .config("spark.sql.adaptive.enabled", "true")
        # Coalesce automático de partições pós-shuffle
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Tamanho limite para broadcast join automático
        .config("spark.sql.autoBroadcastJoinThreshold", 10485760)  # 10 MB
        .getOrCreate()
    )

    # Diretório base do projeto
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Caminho absoluto para o CSV da camada raw
    input_csv = os.path.join(base_dir, "data", "raw", "input_large.csv")

    # Gera data de hoje no formato yyyymmdd para criar a pasta
    hoje = datetime.today().strftime("%Y%m%d")

    # Caminho de saída com data única (ex: data/bronze/20250629/)
    output_parquet = os.path.join(base_dir, "data", "bronze", hoje)

    # Leitura do CSV bruto
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_csv)
    )

    # Tratamentos típicos da camada bronze
    df_bronze = (
        df.dropna(how="all")  # Remove linhas totalmente nulas
          .withColumn("absences", col("absences").cast("int"))  # Garante tipo inteiro
          .withColumn("employee_id", col("employee_id").cast("int"))
          .fillna({"absences": 0})  # Preenche ausências nulas com 0
          .dropDuplicates()  # Elimina duplicatas
          .withColumn("processing_date", lit(hoje))  # Adiciona coluna de data de processamento
    )

    # Salva os dados em Parquet com particionamento por data
    df_bronze.write.mode("overwrite").parquet(output_parquet)

    print(f"Dados bronze salvos em: {output_parquet}")

    # Finaliza a sessão Spark
    spark.stop()

if __name__ == "__main__":
    process_raw_to_bronze()

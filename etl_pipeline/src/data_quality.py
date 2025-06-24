from pyspark.sql import SparkSession, functions as F
import sys

def run_checks():
    spark = SparkSession.builder.appName("data_quality").getOrCreate()

    # 1. Lê dados limpos (Silver)
    df = spark.read.parquet("data/silver/points.parquet")

    # 2. Conta valores nulos por coluna
    nulls = df.select(*[
        F.sum(F.col(c).isNull().cast("int")).alias(c + "_nulls")
        for c in df.columns
    ])
    counts = nulls.collect()[0].asDict()
    print("📊 Null counts por coluna:", counts)

    # 3. Conta possíveis duplicatas por employee e data
    total = df.count()
    deduped = df.dropDuplicates(["employee_id", "date"]).count()
    dups = total - deduped
    print(f"🔁 Duplicados detectados: {dups}")

    # 4. Conta horas negativas como erro de qualidade
    neg = df.filter(F.col("hours") < 0).count()
    print(f"⚠️ Horas negativas encontradas: {neg}")

    # 5. Se houver falha em qualquer categoria, encerra com erro
    if any(counts[c] > 0 for c in counts) or dups > 0 or neg > 0:
        print("❌ Data quality check falhou!")
        sys.exit(1)

    print("✅ Todos quality checks passaram.")
    spark.stop()

if __name__ == "__main__":
    run_checks()

from pyspark.sql import SparkSession

def run_job():
    spark = SparkSession.builder \
        .appName("Test Glue Job Locally") \
        .getOrCreate()

    # Simule les transformations Spark ici
    data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
    columns = ["id", "name"]
    df = spark.createDataFrame(data, columns)

    # Exemple de transformation
    df = df.withColumnRenamed("name", "username")
    df.show()

    spark.stop()

if __name__ == "__main__":
    run_job()

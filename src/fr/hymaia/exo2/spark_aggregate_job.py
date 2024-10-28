from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
import os

def calculate_population_by_departement(df):
    return df.groupBy('departement').agg(count('*').alias('nb_people')).orderBy(col('nb_people').desc(), col('departement'))

def main():
    spark = SparkSession.builder.appName("exo2_aggregate_job").getOrCreate()

    input_path = "data/exo2/clean"
    if not os.path.exists(input_path):
        print(f"Erreur : Le fichier d'entrée {input_path} n'existe pas.")
        return

    df = spark.read.parquet(input_path)

    population_by_dept_df = calculate_population_by_departement(df)

    output_dir = "data/exo2/aggregate"
    population_by_dept_df.coalesce(1).write.mode('overwrite').option("header", "true").csv(output_dir)

    spark.stop()

if __name__ == "__main__":
    main()

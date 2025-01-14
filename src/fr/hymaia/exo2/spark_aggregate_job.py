from pyspark.sql import SparkSession
import pyspark.sql.functions as f

def read_clean_data(spark):
    return spark.read.parquet("data/exo2/clean")

def calculate_population_by_departement(df):
    return df.groupBy("departement") \
             .agg(f.count("*").alias("nb_people")) \
             .orderBy(f.desc("nb_people"), f.asc("departement"))

def write_output(df_result):
    df_result.coalesce(1).write.mode("overwrite").csv("data/exo2/aggregate", header=True)

def main():
    spark = SparkSession.builder \
        .appName("aggregate_job") \
        .master("local[*]") \
        .getOrCreate()

    df_clean = read_clean_data(spark)
    df_population_dept = calculate_population_by_departement(df_clean)
    write_output(df_population_dept)

    spark.stop()

if __name__ == "__main__":
    main()

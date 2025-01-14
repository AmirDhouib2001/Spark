import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def read_files(spark):
    df_clients = spark.read.option("header", True).csv("src/resources/exo2/clients_bdd.csv")
    df_villes = spark.read.option("header", True).csv("src/resources/exo2/city_zipcode.csv")
    return df_clients, df_villes

def filter_major_clients(df):
    return df.filter(f.col("age") >= 18)

def join_with_city(df_clients, df_villes):
    return df_clients.join(df_villes, "zip", "inner") \
                     .select("name", "age", "zip", "city")

def add_departement_column(df):
    return df.withColumn("departement", f.when(f.col("zip").substr(1, 2) == "20", 
                                               f.when(f.col("zip") <= "20190", "2A").otherwise("2B"))
                                      .otherwise(f.col("zip").substr(1, 2)))

def write_output(df_result):
    df_result.write.mode("overwrite").parquet("data/exo2/clean")

def main():
    spark = SparkSession.builder \
        .appName("clean_job") \
        .master("local[*]") \
        .getOrCreate()

    df_clients, df_villes = read_files(spark)
    df_clients_majeurs = filter_major_clients(df_clients)
    df_clients_villes = join_with_city(df_clients_majeurs, df_villes)
    df_clients_dept = add_departement_column(df_clients_villes)
    write_output(df_clients_dept)
    
    spark.stop()

if __name__ == "__main__":
    main()

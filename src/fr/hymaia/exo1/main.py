import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder \
        .appName("wordcount") \
        .master("local[*]") \
        .getOrCreate()
        
    df = spark.read.option("header", True).csv("src/resources/exo1/data.csv")
    
    result_df = wordcount(df, "text")
    result_df.show()
    
    
    result_df.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")
    
    spark.stop()


def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()


if __name__ == "__main__":
    main()
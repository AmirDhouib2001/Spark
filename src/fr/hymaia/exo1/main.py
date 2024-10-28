import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def main():
    # Créer une SparkSession en mode local utilisant tous les cœurs disponibles
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("wordcount") \
        .getOrCreate()
    

    # Lire le fichier CSV
    df = spark.read.csv('/spark/spark-handson/src/resources/exo1/data.csv', header=True)

    result_df = wordcount(df, 'text')  
    result_df.show()
    count=6
    df_test=countword(result_df,count)
    df_test.show()


    result_df.write \
        .mode("overwrite") \
        .partitionBy('count') \
        .parquet('data/exo1/output')

    spark.stop()

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()
def countword(df,count):
    return  df.filter(f.col("count")==count)
    

if __name__ == "__main__":
    main()

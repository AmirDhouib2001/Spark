from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# SparkSession
spark = SparkSession.builder.appName("No UDF").getOrCreate()

# Chargement des données
sell_df = spark.read.csv('resources/exo4/sell.csv', header=True, inferSchema=True)

# Utilisation de `when` et `otherwise` pour ajouter `category_name`
sell_df = sell_df.withColumn("category_name",
                             when(sell_df["category"] < 6, "food").otherwise("furniture"))
sell_df.show()
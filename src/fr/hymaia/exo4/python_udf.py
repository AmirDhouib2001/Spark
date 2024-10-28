from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Initialisation de la Spark session
spark = SparkSession.builder.appName("Python UDF").getOrCreate()

# Définition de l’UDF
def get_category_name(category):
    return 'food' if category < 6 else 'furniture'

# Enregistrement de l’UDF
category_name_udf = udf(get_category_name, StringType())

# Chargement des données
sell_df = spark.read.csv('resources/exo4/sell.csv', header=True, inferSchema=True)

# Application de l’UDF pour ajouter la colonne `category_name`
sell_df = sell_df.withColumn("category_name", category_name_udf(sell_df["category"]))
sell_df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Fonction pour convertir les catégories en noms
def category_to_name(category):
    return "food" if category < 6 else "furniture"

# Création de la SparkSession
spark = SparkSession.builder \
    .appName("Python UDF Example") \
    .getOrCreate()

# Lecture des données (utilise un chemin absolu)
df = spark.read.csv("/spark/spark-handson/src/resources/exo4/sell.csv", header=True, inferSchema=True)

# Définition et application de la UDF
category_udf = udf(category_to_name, StringType())
df = df.withColumn("category_name", category_udf(df["category"]))

# Affichage des résultats
df.show()
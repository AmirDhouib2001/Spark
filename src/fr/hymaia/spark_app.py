from pyspark.sql import SparkSession

# Création de la SparkSession
spark = SparkSession.builder \
    .appName("Exemple Spark Job") \
    .getOrCreate()

# Exemple simple de traitement : création et affichage d'un DataFrame
data = [("Alice", 29), ("Bob", 35), ("Cathy", 42)]
columns = ["Name", "Age"]

df = spark.createDataFrame(data, columns)
df.show()

# Arrêt de la SparkSession
spark.stop()

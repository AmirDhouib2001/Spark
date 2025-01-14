from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq

# Création de la SparkSession avec le JAR Scala
spark = SparkSession.builder \
    .appName("Scala UDF Example") \
    .config("spark.jars", "/opt/spark/spark-handson/src/resources/exo4/udf.jar") \
    .getOrCreate()

# Définir une fonction pour appeler la UDF Scala
def addCategoryName(col):
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

# Lecture des données (utilise un chemin absolu)
df = spark.read.csv("/opt/spark/spark-handson/src/resources/exo4/sell.csv", header=True, inferSchema=True)

# Application de la UDF Scala
df = df.withColumn("category_name", addCategoryName(df["category"]))

# Affichage des résultats
df.show()
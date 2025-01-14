from pyspark.sql import SparkSession
from pyspark.sql.functions import when, sum
from pyspark.sql.window import Window

# Création de la session Spark
spark = SparkSession.builder.appName("No UDF Example").getOrCreate()

# Lecture du fichier de données
df = spark.read.csv("/spark/spark-handson/src/resources/exo4/sell.csv", header=True, inferSchema=True)

# Ajout de la colonne `category_name` sans utiliser d'UDF
df = df.withColumn("category_name", when(df['category'] < 6, 'food').otherwise('furniture'))

# Définition de la fenêtre pour le calcul par jour et par catégorie
window_spec_day = Window.partitionBy("date", "category_name")

# Calcul de la somme des prix par jour et par catégorie
df = df.withColumn("total_price_per_category_per_day", sum("price").over(window_spec_day))

# Définition de la fenêtre pour le calcul sur les 30 derniers jours
window_spec_30_days = Window.partitionBy("category_name").orderBy("date").rowsBetween(-30, 0)

# Calcul de la somme des prix sur 30 jours
df = df.withColumn("total_price_per_category_per_day_last_30_days", sum("price").over(window_spec_30_days))

# Affichage des résultats
df.show()

# Arrêt de la session Spark
spark.stop()

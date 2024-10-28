from pyspark.sql import SparkSession
from pyspark.sql.functions import when
import time

# Initialisation de la Spark session
spark = SparkSession.builder.appName("No UDF").getOrCreate()

# Chargement des données
sell_df = spark.read.csv('src/resources/exo4/sell.csv', header=True, inferSchema=True)

# Mesurer le temps d'exécution
start_time = time.time()

# Application de la logique sans UDF
sell_df = sell_df.withColumn(
    "category_name",
    when(sell_df["category"] < 6, "food").otherwise("furniture")
)

end_time = time.time()
print(f"Temps d'exécution du No UDF : {end_time - start_time:.4f} secondes")

sell_df.show()
spark.stop()

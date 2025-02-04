from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import time

def main():
    # Début du timer
    start_time = time.time()

    # Arrêter tout SparkContext actif pour éviter les conflits
    if SparkContext._active_spark_context:
        SparkContext._active_spark_context.stop()

    # Création de la session Spark
    spark = SparkSession.builder.appName("Python UDF Example").getOrCreate()

    # Définition de la fonction UDF
    def get_category_name(category):
        if category < 6:
            return 'food'
        else:
            return 'furniture'

    # Enregistrement de l'UDF
    category_name_udf = udf(get_category_name, StringType())

    # Lecture du fichier de données
    print("Chargement des données...")
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

    # Application de l'UDF
    print("Application de l'UDF...")
    df = df.withColumn("category_name", category_name_udf(df['category']))

    # Affichage des résultats
    print("Résultats après application de l'UDF :")
    df.show()

    # Fin du timer
    end_time = time.time()
    print(f"Temps d'exécution total : {end_time - start_time:.2f} secondes")

    # Arrêt de la session Spark
    spark.stop()

if __name__ == "__main__":
    main()
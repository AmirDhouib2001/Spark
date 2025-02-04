from pyspark.sql import SparkSession, Column
from pyspark.sql.functions import col
from pyspark.sql.column import _to_java_column, _to_seq
import time

def main():
    # Début du timer
    start_time = time.time()

    # Création de la session Spark avec configuration pour le JAR Scala
    spark = SparkSession.builder \
        .appName("Scala UDF Example") \
        .config("spark.jars", "src/resources/exo4/udf.jar") \
        .getOrCreate()

    # Définition de la fonction pour appeler l'UDF Scala
    def addCategoryName(col):
        sc = spark.sparkContext
        add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

    # Lecture du fichier de données
    print("Chargement des données...")
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

    # Ajout de la colonne `category_name` via l'UDF Scala
    print("Application de l'UDF Scala...")
    df = df.withColumn("category_name", addCategoryName(col("category")))

    # Affichage des résultats
    print("Résultats après application de l'UDF Scala :")
    df.show()

    # Fin du timer
    end_time = time.time()
    print(f"Temps d'exécution total : {end_time - start_time:.2f} secondes")

    # Arrêt de la session Spark
    spark.stop()

if __name__ == "__main__":
    main()
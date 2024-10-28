from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time

def main():
    # SparkSession avec le JAR Scala
    spark = SparkSession.builder \
        .appName("Scala UDF") \
        .config('spark.jars', 'src/resources/exo4/udf.jar') \
        .getOrCreate()

    # Fonction pour accéder à l’UDF Scala
    def addCategoryName(col):
        sc = spark.sparkContext
        add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
        return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

    # Chargement des données
    sell_df = spark.read.csv('src/resources/exo4/sell.csv', header=True, inferSchema=True)

    # Mesurer le temps d'exécution
    start_time = time.time()

    # Application de l’UDF Scala pour ajouter la colonne `category_name`
    sell_df = sell_df.withColumn("category_name", addCategoryName(sell_df["category"]))

    end_time = time.time()
    print(f"Temps d'exécution du Scala UDF : {end_time - start_time:.4f} secondes")

    sell_df.show()
    spark.stop()

if __name__ == "__main__":
    main()

# import pyspark.sql.functions as f
# from pyspark.sql import SparkSession


# spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()

# def main():
#     print("Hello world!")

#import pyspark.sql.functions as f
#from pyspark.sql import SparkSession
#spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()
#def main():
#    print("Hello world!")

from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq

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
sell_df = spark.read.csv('resources/exo4/sell.csv', header=True, inferSchema=True)

# Application de l’UDF Scala pour ajouter la colonne `category_name`
sell_df = sell_df.withColumn("category_name", addCategoryName(sell_df["category"]))
sell_df.show()
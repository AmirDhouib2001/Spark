from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def filter_adult_clients(df):
    return df.filter(col('age') >= 18)

def join_clients_with_cities(clients_df, villes_df):
    return clients_df.join(villes_df, clients_df['zip'] == villes_df['zip'], 'inner') \
                     .select(clients_df['name'], clients_df['age'], clients_df['zip'], villes_df['city'])


def add_departement_column(df):
    return df.withColumn(
        'departement',
        when(col('zip').startswith('2') & (col('zip').cast('int') <= 20190), '2A')
        .when(col('zip').startswith('2') & (col('zip').cast('int') > 20190), '2B')
        .otherwise(col('zip').substr(1, 2))
    )
def main():
    spark = SparkSession.builder.appName("exo2_clean_job").getOrCreate()

    clients_df = spark.read.option("header", "true").csv("src/resources/exo2/clients_bdd.csv")
    villes_df = spark.read.option("header", "true").csv("src/resources/exo2/city_zipcode.csv")

    clients_adult_df = filter_adult_clients(clients_df)

    clients_with_city_df = join_clients_with_cities(clients_adult_df, villes_df)

    clients_with_city_dept_df = add_departement_column(clients_with_city_df)

    output_path = "data/exo2/clean"

    clients_with_city_dept_df.write.mode('overwrite').parquet(output_path)
    

    spark.stop()

if __name__ == "__main__":
    main()
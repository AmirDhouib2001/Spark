import unittest
from pyspark.sql import Row
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_clean_job import filter_adult_clients, join_clients_with_cities, add_departement_column
from src.fr.hymaia.exo2.spark_aggregate_job import calculate_population_by_departement

class TestIntegrationJobs(unittest.TestCase):

    def test_full_workflow(self):
        clients_df = spark.createDataFrame([
            Row(name='Amir', age=22, zip='94140'),
            Row(name='test', age=19, zip='77002'),
            Row(name='Dhouib', age=25, zip='20167'),
            Row(name='Dhouib2', age=16, zip='99999')

        ])
        cities_df = spark.createDataFrame([
            Row(zip='94140', city='Alfortville'),
            Row(zip='77002', city='Lyon'),
            Row(zip='20167', city='Ajaccio'),
            Row(zip='99999', city='Sfax')

        ])

        filtered_clients_df = filter_adult_clients(clients_df)

        clients_with_city_df = join_clients_with_cities(filtered_clients_df, cities_df)

        clients_with_city_dept_df = add_departement_column(clients_with_city_df)

        population_by_dept_df = calculate_population_by_departement(clients_with_city_dept_df)

        expected_df = spark.createDataFrame([
            Row(departement='77', nb_people=1),
            Row(departement='2A', nb_people=1),
            Row(departement='94', nb_people=1)
        ])

        self.assertCountEqual(population_by_dept_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()

import unittest
from pyspark.sql import Row
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_clean_job import filter_major_clients, join_with_city, add_departement_column

class TestCaseClean(unittest.TestCase):
    
    def test_filter_adult_clients(self):
        input_df = spark.createDataFrame([
            Row(name='Amir', age=17, zip='94140'),
            Row(name='test', age=19, zip='77002'),
            Row(name='Dhouib', age=15, zip='77003')
        ])
        expected_df = spark.createDataFrame([
            Row(name='test', age=19, zip='77002')
        ])

        actual_df = filter_major_clients(input_df)

        expected_schema = [('name', 'string'), ('age', 'bigint'), ('zip', 'string')]
        self.assertEqual(actual_df.dtypes, expected_schema)

        self.assertCountEqual(actual_df.collect(), expected_df.collect())

    def test_join_clients_with_cities(self):
        clients_df = spark.createDataFrame([
            Row(name='Amir', age=19, zip='94140'),
            Row(name='test', age=22, zip='77002')
        ])
        cities_df = spark.createDataFrame([
            Row(zip='94140', city='Alfortville'),
            Row(zip='77002', city='Lyon')
        ])
        expected_df = spark.createDataFrame([
            Row(name='Amir', age=19, zip='94140', city='Alfortville'),
            Row(name='test', age=22, zip='77002', city='Lyon')
        ])

        actual_df = join_with_city(clients_df, cities_df)

        expected_schema = [('name', 'string'), ('age', 'bigint'), ('zip', 'string'), ('city', 'string')]
        self.assertEqual(actual_df.dtypes, expected_schema)

        self.assertCountEqual(actual_df.collect(), expected_df.collect())

    def test_add_departement_column(self):
        input_df = spark.createDataFrame([
            Row(name='Amir', age=19, zip='94140'),
            Row(name='test', age=22, zip='20167'),
            Row(name='Dhouib', age=25, zip='20200')
        ])
        expected_df = spark.createDataFrame([
            Row(name='Amir', age=19, zip='94140', departement='94'),
            Row(name='test', age=22, zip='20167', departement='2A'),
            Row(name='Dhouib', age=25, zip='20200', departement='2B')
        ])

        actual_df = add_departement_column(input_df)

        expected_schema = [('name', 'string'), ('age', 'bigint'), ('zip', 'string'), ('departement', 'string')]
        self.assertEqual(actual_df.dtypes, expected_schema)

        self.assertCountEqual(actual_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()

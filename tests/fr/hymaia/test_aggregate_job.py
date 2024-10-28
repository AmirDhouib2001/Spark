import unittest
from pyspark.sql import Row
from tests.fr.hymaia.spark_test_case import spark
from src.fr.hymaia.exo2.spark_aggregate_job import calculate_population_by_departement

class TestCaseAggregate(unittest.TestCase):
    
    def test_calculate_population_by_departement(self):
        input_df = spark.createDataFrame([
            Row(name='Amir', age=19, zip='94140', city='Alfortville', departement='94'),
            Row(name='test', age=22, zip='77150', city='Paris', departement='77'),
            Row(name='Dhouib', age=25, zip='20167', city='Ajaccio', departement='2A')
        ])
        
        expected_df = spark.createDataFrame([
            Row(departement='94', nb_people=1),
            Row(departement='2A', nb_people=1),
            Row(departement='77', nb_people=1)
        ])

        actual_df = calculate_population_by_departement(input_df)

        expected_schema = [('departement', 'string'), ('nb_people', 'bigint')]
        self.assertEqual(actual_df.dtypes, expected_schema)

        self.assertCountEqual(actual_df.collect(), expected_df.collect())

if __name__ == "__main__":
    unittest.main()

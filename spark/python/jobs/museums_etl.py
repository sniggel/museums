"""
museums_etl.py
~~~~~~~~~~

This Python module contains an example Apache Spark ETL job definition
that implements best practices for production ETL jobs. It can be
submitted to a Spark cluster (or locally) using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions
(necessary for running any Spark job, locally or otherwise). For
example, this example script can be executed as follows,

    (Other)
    $SPARK_HOME/bin/spark-submit \
    --master spark://localhost:7077 \
    --py-files packages.zip \
    --files configs/museums_etl_config.json \
    jobs/museums_etl.py

    (Local)
    $SPARK_HOME/bin/spark-submit \
    --py-files packages.zip \
    --files configs/museums_etl_config.json \
    jobs/museums_etl.py

where packages.zip contains Python modules required by ETL job (in
this example it contains a class to provide access to Spark's logger),
which need to be made available to each executor process on every node
in the cluster; museums_etl_config.json is a text file sent to the cluster,
containing a JSON object with all of the configuration parameters
required by the ETL job; and, museums_etl.py contains the Spark application
to be executed by a driver process on the Spark master node.

For more details on submitting Spark applications, please see here:
http://spark.apache.org/docs/latest/submitting-applications.html

Our chosen approach for structuring jobs is to separate the individual
'units' of ETL - the Extract, Transform and Load parts - into dedicated
functions, such that the key Transform steps can be covered by tests
and jobs or called from within another environment (e.g. a Jupyter or
Zeppelin notebook).
"""

import json

from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col
from pyspark.sql.types import *

from dependencies.spark import start_spark
from dependencies.wikipedia_utils import fetch_museums_stats
from dependencies.wikipedia_utils import fetch_city_population
from dependencies.wikipedia_utils import read_largest_cities_table
from dependencies.wikipedia_utils import read_museums_table


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='museums_etl',
        files=['configs/museums_etl_config.json'])

    # log that main ETL job is starting
    log.info('etl_job is up-and-running')

    # execute ETL pipeline

    # load all museums
    museums_df = load_museums(spark)

    # for every museums, fetch the characteristics
    museums_characteristics_df = load_museums_characteristics(spark, museums_df)

    # load most populated cities
    cities_df = load_largest_cities(spark)

    # join museum dataframe with cities dataframe
    museums_with_population_df = museums_df.join(cities_df, museums_df.City == cities_df['City Name'], how='left')

    # fill missing population
    missing_population_df = load_missing_population(spark, museums_with_population_df)

    # merge museums_with_population_df with missing_population_df
    museums_with_missing_population_df = museums_with_population_df \
        .join(missing_population_df, museums_with_population_df.City == missing_population_df['Missing Name'],
              how='left')

    # merge museums_with_missing_population_df with museums_characteristics_df
    museums_with_characteristics_df = museums_with_missing_population_df \
        .join(museums_characteristics_df,
              museums_with_missing_population_df.Name == museums_characteristics_df['Museum Name'], how='left')

    # create final dataframe which is going to be used to create the database
    df = museums_with_characteristics_df \
        .select('Name', 'WikiLink', 'City', 'Visitors per year', 'Museum Characteristics', 'Population',
                'Missing Population') \
        .withColumn('City Population', coalesce(museums_with_characteristics_df['Missing Population'],
                                                museums_with_characteristics_df['Population'])) \
        .select(col('Name').alias('name'), col('WikiLink').alias('wikilink'), col('City').alias('city'),
                col('Visitors per year').alias('visitors'), col('Museum Characteristics').alias('characteristics'),
                col('City Population').alias('city_population')).where('visitors > 2000000')
    df.show()

    # Write all rows to postgres
    write_to_postgresql(df, config)

    # log the success and terminate Spark application
    log.info('test_etl_job is finished')
    spark.stop()
    return None


def write_to_postgresql(df, config):
    df.write.format('jdbc')\
        .option('url',
                f"jdbc:postgresql://{config['database_address']}/{config['database_name']}?user={config['database_username']}"
                f"&password={config['database_password']}")\
        .option('driver', config['jdbc_driver_class'])\
        .option('dbtable', config['database_table_name'])\
        .mode('append')\
        .save()


def load_missing_population(spark, museums_with_population_df):
    sc = spark.sparkContext
    missing_cities_list = museums_with_population_df \
        .select('City', 'Population', 'City Wikilink') \
        .where('Population is null or Population == 0') \
        .distinct() \
        .collect()
    name_and_population = []
    missing_cities_schema = StructType([
        StructField('Missing Name', StringType(), True),
        StructField('Missing Population', StringType(), True),
    ])
    for city in missing_cities_list:
        wiki_link = city['City Wikilink']
        city_population = fetch_city_population(wiki_link)
        city_name = city['City']
        name_and_population.append([city_name, city_population])
    rdd = sc.parallelize(name_and_population)
    missing_cities_df = spark.createDataFrame(data=rdd, schema=missing_cities_schema)
    return missing_cities_df


def load_largest_cities(spark):
    sc = spark.sparkContext
    cities = read_largest_cities_table()
    cities_schema = StructType([
        StructField('City Name', StringType(), True),
        StructField('Population', IntegerType(), True)
    ])
    rdd = sc.parallelize(cities)
    cities_df = spark.createDataFrame(data=rdd, schema=cities_schema)
    return cities_df


def load_museums(spark):
    sc = spark.sparkContext
    museums = read_museums_table()
    museums_schema = StructType([
        StructField('Name', StringType(), True),
        StructField('Wikilink', StringType(), True),
        StructField('City', StringType(), True),
        StructField('City Wikilink', StringType(), True),
        StructField('Visitors per year', IntegerType(), True),
        StructField('Year Reported', IntegerType(), True)
    ])
    rdd = sc.parallelize(museums)
    df = spark.createDataFrame(data=rdd, schema=museums_schema)
    return df


def load_museums_characteristics(spark, museums_df):
    sc = spark.sparkContext
    characteristics = []
    characteristics_schema = StructType([
        StructField('Museum Name', StringType(), True),
        # as json
        StructField('Museum Characteristics', StringType(), True)
    ])
    # since the list is pretty small, we can invoke collect here.
    # If I had more time, I would do it another way.
    for row in museums_df.collect():
        museum_name = row.Name
        museum_link = row.Wikilink
        museum_characteristics = fetch_museums_stats(museum_link)
        museum_characteristics_json = json.dumps(museum_characteristics, ensure_ascii=False)
        characteristics.append([museum_name, museum_characteristics_json])
    rdd = sc.parallelize(characteristics)
    characteristics_df = spark.createDataFrame(data=rdd, schema=characteristics_schema)
    return characteristics_df


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()

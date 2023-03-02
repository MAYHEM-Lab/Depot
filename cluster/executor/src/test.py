import pyspark
from pyspark import SparkConf

conf = SparkConf()
spark_master = "localhost:7077"
spark = pyspark.sql.SparkSession.builder.master("spark://localhost:7077").config(conf=conf).getOrCreate()
print(spark)
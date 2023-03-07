from pyspark.sql import SparkSession
from dltx.utils import get_param

spark = SparkSession.builder.getOrCreate()
print(get_param("hello"))


import json
from pyspark.sql import SparkSession
from dltx.utils import get_param, file_path

spark = SparkSession.builder.getOrCreate()
print("getting parameter:", get_param("hello"))
with open(file_path("myconf.json"), "rb") as f:
    data = json.load(f)
    print("resource contents:", data)

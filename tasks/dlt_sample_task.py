# TODO: provide dlt stubs and mocks for unit tests?
import dlt
from datetime import datetime, date
from pyspark.sql import SparkSession, Row
from dltx.utils import table_name, get_param

spark = SparkSession.builder.getOrCreate()


# DLT example table with suffix
@dlt.table(name=table_name("dlt_experiment_one"))
def dlt_experiment_one():
    print(get_param("hello"))
    return spark.createDataFrame([
        Row(a=1, b=4., c='GFG1', d=date(2000, 8, 1), e=datetime(2000, 8, 1, 12, 0)),
        Row(a=2, b=8., c='GFG2', d=date(2000, 6, 2), e=datetime(2000, 6, 2, 12, 0)),
        Row(a=4, b=5., c='GFG3', d=date(2000, 5, 3), e=datetime(2000, 5, 3, 12, 0))
    ])

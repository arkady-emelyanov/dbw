from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from typing import Any


def get_param(name, default=None) -> Any:
    spark = SparkSession.getActiveSession()
    default_value = default if default else None

    dbutils = DBUtils(spark)
    result = None
    try:
        result = dbutils.widgets.get(name)
    except:
        pass
    if not result:
        result = spark.conf.get(name, default_value)
    return result


def table_name(name) -> str:
    suffix = get_param("dwb.use_name_suffix")
    if suffix:
        return f"{name}_{suffix}"
    else:
        return name

from pyspark.sql import SparkSession
from pyspark.sql import types as X
from pyspark.sql.functions import sum
import os
spark = (SparkSession.builder
         .master('local')
         .appName('another one')
         .getOrCreate())

sc = spark.sparkContext


def get_schema(*args):
    return X.StructType([
        X.StructField(*arg)
        for arg in args
    ])

s = [("Order Number",X.IntegerType(),False),
     ("id", X.IntegerType(), False)]




print(get_schema(*s))

def give_me_data(file1,schema):
    return spark.read.csv(
        path=file1,
        schema=get_schema(*schema)
    )

file_nm = "yo.txt"

path = os.path.dirname(os.getcwd())
file_path = path + "/" + file_nm

df = give_me_data(file_path,s)

print(df.agg(sum("id")).show())-- there is difference between sum func and sum func in pyspark
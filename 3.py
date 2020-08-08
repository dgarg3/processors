from pyspark.sql import SparkSession
from pyspark.sql import types as x
from pyspark.sql import functions as f

spark = (SparkSession.builder
         .master("local")
         .appName("yell")
         .getOrCreate()
         )

sc = spark.sparkContext

sch = x.StructType([
    x.StructField("Order Number",x.StringType()),
    x.StructField("Order qty",x.IntegerType())
])

dt1 = [("Abc",100),("DEF",300),("DEF",200)]

df = spark.createDataFrame(
    schema=sch,
    data=dt1
)

print(df.show())

df1 = df.groupBy("Order Number").agg(f.sum("Order Qty").alias("Total Qty"))
#df1.write.format('csv').option('header', True).mode('overwrite').option('sep', ',').save('abc.txt')

def write_to_csv(df):
    df.write.format('csv').option('header', True).mode('overwrite').option('sep', ',').save('abc.txt')

write_to_csv(df1)


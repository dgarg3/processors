import pyspark
sc = pyspark.SparkContext('local')

f = sc.textFile('yo.txt')

s = (f.filter(lambda w : 'd' in w).count())

print("tum aa gaye ho: %i" %s)




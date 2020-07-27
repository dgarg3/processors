from pyspark import SparkContext,SparkConf
conf = (SparkConf()
        .setMaster("local")
        .setAppName("yole")
        )
sc = SparkContext(conf=conf)

f = sc.textFile('yo.txt')

s = (f.filter(lambda w : 'd' in w).count())

print("tum aa gaye ho: %i" %s)
f1 = [1,2,3]

x1 = sc.parallelize(f1)

t = x1.count()

print("total number of elements are %i" %t)

f1grtr2 = x1.filter(lambda x : x>=2)
t1 = f1grtr2.collect()

print ("number of digits greater than 2 %s" % (t1))






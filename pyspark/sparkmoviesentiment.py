from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row

conf = SparkConf().setAppName("sparkmoviesentiment")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

inrdd = sc.textFile("/usr/azure/twitterdata/Tweets*")
inrddsplt = inrdd.map(lambda line: line.split("|"))
inrddspltflt = inrddsplt.filter(lambda spltln: spltln[5] == "None")
sentdf = inrddspltflt.map(lambda spltln: Row(movie=spltln[0],sentiment=spltln[8]))

sentids = sqlContext.createDataFrame(sentdf)
sentids.printSchema()

sentidsout = sentids.groupBy("movie", "sentiment").count()
sentidsout.printSchema()
sentidsordered = sentidsout.orderBy("movie","sentiment")
sentidsordered.show()
sentidsordered.write.format("orc").option("path","/usr/azure/moviesentiment").mode("overwrite").saveAsTable("moviesentiment")


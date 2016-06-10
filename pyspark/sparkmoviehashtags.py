from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row

conf = SparkConf().setAppName("sparkmoviehashtags")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

inrdd = sc.textFile("/usr/azure/twitterdata/Tweets*")
inrddsplt = inrdd.map(lambda line: line.split("|"))
inrddspltflt = inrddsplt.filter(lambda spltln: spltln[5] == "None")
inrddspltfltwrds = inrddsplt.filter(lambda spltln: spltln[7] != '')
inrdddf = inrddspltfltwrds.map(lambda spltln: (spltln[0],spltln[7].split(",")))

inrddfmapv = inrdddf.flatMapValues(lambda x: x)
hashtgdf = inrddfmapv.map(lambda hashtag: Row(movie=hashtag[0],hashtags=hashtag[1]))

hashtgds = sqlContext.createDataFrame(hashtgdf)
hashtgds.printSchema()

hashtgdsout = hashtgds.groupBy("movie", "hashtags").count()
hashtgdsout.printSchema()
hashtgdsordered = hashtgdsout.orderBy("movie","count",ascending=False)
hashtgdsordered.show()

hashtgdsordered.write.format("orc").option("path","/usr/azure/moviehashtags").mode("overwrite").saveAsTable("moviehashtags")

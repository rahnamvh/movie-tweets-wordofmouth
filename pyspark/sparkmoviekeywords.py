from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row

conf = SparkConf().setAppName("sparkmoviekeywords")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

inrdd = sc.textFile("/usr/azure/twitterdata/Tweets*")
inrddsplt = inrdd.map(lambda line: line.split("|"))
inrddspltflt = inrddsplt.filter(lambda spltln: spltln[5] == "None")
inrddspltfltwrds = inrddsplt.filter(lambda spltln: spltln[8] != "neutral")
inrdddf = inrddspltfltwrds.map(lambda spltln: (spltln[0],spltln[9].replace('_',' ').split(",")))

inrddfmapv = inrdddf.flatMapValues(lambda x: x)
keyworddf = inrddfmapv.map(lambda keyword: Row(movie=keyword[0],keywords=keyword[1]))

keywordds = sqlContext.createDataFrame(keyworddf)
keywordds.printSchema()

keyworddsout = keywordds.groupBy("movie", "keywords").count()
keyworddsout.printSchema()
keyworddsordered = keyworddsout.orderBy("movie","count",ascending=False)
keyworddsordered.show()

keyworddsordered.write.format("orc").option("path","/usr/azure/moviekeywords").mode("overwrite").saveAsTable("moviekeywords")

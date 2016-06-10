from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext, Row

conf = SparkConf().setAppName("sparkmoviementions")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

inrdd = sc.textFile("/usr/azure/twitterdata/Tweets*")
inrddsplt = inrdd.map(lambda line: line.split("|"))
inrddspltflt = inrddsplt.filter(lambda spltln: spltln[5] == "None")
inrddspltfltwrds = inrddsplt.filter(lambda spltln: spltln[6] != '')
inrdddf = inrddspltfltwrds.map(lambda spltln: (spltln[0],spltln[6].split(",")))

inrddfmapv = inrdddf.flatMapValues(lambda x: x)
mentiondf = inrddfmapv.map(lambda mention: Row(movie=mention[0],mentions=mention[1]))

mentionds = sqlContext.createDataFrame(mentiondf)
mentionds.printSchema()

mentiondsout = mentionds.groupBy("movie", "mentions").count()
mentiondsout.printSchema()
mentiondsordered = mentiondsout.orderBy("movie","count",ascending=False)
mentiondsordered.show()

mentiondsordered.write.format("orc").option("path","/usr/azure/moviementions").mode("overwrite").saveAsTable("moviementions")

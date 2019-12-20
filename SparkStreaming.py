import findspark


findspark.init('/home/alex/bigdata/spark-2.4.4-bin-hadoop2.7')

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext('local[2]', 'SparkStreamintWordCount')

sc.setLogLevel("INFO")

#We pass the Spark Context, the 1 is the interval for the batches
ssc = StreamingContext(sc, 10)

lines = ssc.socketTextStream('127.0.0.1',9999)

words = lines.flatMap(lambda line: line.split(' '))
pairs = words.map(lambda word: (word, 1))
word_counts = pairs.reduceByKey(lambda num1,num2: num1+num2)
word_counts.pprint()

ssc.start()
ssc.awaitTermination()
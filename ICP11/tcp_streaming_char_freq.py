from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "TCP Streaming Char Frequency")
ssc = StreamingContext(sc, 5)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 8080)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Map each word and its length in each batch
pairs = words.map(lambda word: (len(word), word.lower()))
# pairs.pprint()
wordCounts = pairs.reduceByKey(lambda x, y: x + "," + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

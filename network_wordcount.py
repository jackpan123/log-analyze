import sys
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    spark = SparkSession(sc)
    lines = (spark
             .readStream.format("socket")
             .option("host", "localhost")
             .option("port", 9999)
             .load())

    words = lines.select(F.split(F.col("value"), "\\s").alias("word"))
    counts = words.groupBy("word").count()
    checkpointDir = "/Users/jackpan/JackPanDocuments/temporary/checkPointTest"
    streamingQuery = (counts
                      .writeStream
                      .format("console")
                      .outputMode("complete")
                      .trigger(processingTime="1 second")
                      .option("checkpointLocation", checkpointDir)
                      .start())
    streamingQuery.awaitTermination()


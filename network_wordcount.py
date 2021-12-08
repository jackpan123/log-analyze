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

    # words = lines.select(F.split(F.col("value"), "\\s").alias("word"))
    words = lines.select(F.split(F.col("value"), "\\n").cast("string").alias("word"))
    logLines = words.filter(words["word"].rlike(r'URI:.*最大内存:.*已分配内存:.*最大可用内存:.*'))
    checkpointDir = "/Users/jackpan/JackPanDocuments/temporary/checkPointTest"
    outputDir = "/Users/jackpan/JackPanDocuments/temporary/out-log"
    streamingQuery = (logLines
                      .writeStream
                      .format("parquet")
                      .option("path", outputDir)
                      .outputMode("append")
                      .trigger(processingTime="10 second")
                      .option("checkpointLocation", checkpointDir)
                      .start())
    streamingQuery.awaitTermination()


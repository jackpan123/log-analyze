from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import udf
from pyspark.sql.functions import sum as spark_sum
import re
import pandas as pd
import glob

from pyspark.sql.types import StringType

sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

raw_data_files = glob.glob('/Users/jackpan/JackPanDocuments/temporary/tet/edp.2021-11-29.out')
base_df = spark.read.text(raw_data_files)
normal_log_df = base_df.filter(base_df['value'].rlike(r'URI:.*最大内存:.*已分配内存:.*最大可用内存:.*'))

sample_normal_log = [item['value'] for item in normal_log_df.take(15)]
print(sample_normal_log)
# date_time_pattern = r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3}'
# date_time_list = [re.search(date_time_pattern, item).group(1) for item in sample_normal_log]
# print(date_time_list)

ts_pattern = r'(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}\.\d{3})'
timestamps = [re.search(ts_pattern, item).group(1) for item in sample_normal_log]
print(timestamps)

spend_time_pattern = r'(耗时：\d+:\d+:\d+\.\d+)'
spend_time = [re.search(spend_time_pattern, item).group(1) for item in sample_normal_log]
print(spend_time)

request_uri_pattern = r'((\/\w+)+)'
request_uri_list = [re.search(request_uri_pattern, item).group(1) for item in sample_normal_log]
print(request_uri_list)

# 最大内存
max_memory_pattern = r'(最大内存: \d+m)'
max_memory_list = [re.search(max_memory_pattern, item).group(1) for item in sample_normal_log]
print(max_memory_list)

# 已分配内存
already_allow_memory_pattern = r'(已分配内存: \d+m)'
already_allow_memory_list = [re.search(already_allow_memory_pattern, item).group(1) for item in sample_normal_log]
print(already_allow_memory_list)

# 已分配内存中的剩余空间
already_allow_memory_free_pattern = r'(已分配内存中的剩余空间: \d+m)'
already_allow_memory_free_list = [re.search(already_allow_memory_free_pattern, item).group(1) for item in
                                  sample_normal_log]
print(already_allow_memory_free_list)

# 最大可用内存
max_useful_memory_free_pattern = r'(最大可用内存: \d+m)'
already_allow_memory_free_list = [re.search(max_useful_memory_free_pattern, item).group(1) for item in
                                  sample_normal_log]
print(already_allow_memory_free_list)


def count_seconds(col_name):
    time_arr = col_name.split(':')
    print(time_arr[0])
    return int(time_arr[0] * 3600) + int(time_arr[1] * 60) + int(float(time_arr[2]))


count_seconds_udf = udf(lambda z: count_seconds(z), StringType())
performance_log_df = normal_log_df.select(
    regexp_extract('value', ts_pattern, 1).alias('time'),
    regexp_extract('value', spend_time_pattern, 1).alias('spend_time'),
    regexp_extract('value', request_uri_pattern, 1).alias('request_uri'),
    regexp_extract('value', max_memory_pattern, 1).alias('max_memory'),
    regexp_extract('value', already_allow_memory_pattern, 1).alias('total_memory'),
    regexp_extract('value', already_allow_memory_free_pattern, 1).alias('free_memory'),
    regexp_extract('value', max_useful_memory_free_pattern, 1).alias('max_can_use_memory'),
).withColumn("spend_time", regexp_replace('spend_time', '耗时：', '')) \
    .withColumn("spend_time", count_seconds_udf('spend_time')) \
    .withColumn("max_memory", regexp_replace('max_memory', '(最大内存: |m)', '')) \
    .withColumn("total_memory", regexp_replace('total_memory', '(已分配内存: |m)', '')) \
    .withColumn("free_memory", regexp_replace('free_memory', '(已分配内存中的剩余空间: |m)', '')) \
    .withColumn("max_can_use_memory", regexp_replace('max_can_use_memory', '(最大可用内存: |m)', ''))

performance_log_df.show(10, truncate=False)

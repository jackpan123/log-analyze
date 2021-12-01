from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import sum as spark_sum
import re
import pandas as pd
import glob

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
# normal_log_df.select(
#     regexp_extract('value', spend_time_pattern, 1).alias('spend_time'),
#     regexp_extract('value', spend_time_pattern,  1).alias('spend_time'),
#     regexp_extract('value', spend_time_pattern,  1).alias('spend_time'),
# )
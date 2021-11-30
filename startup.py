import inline as inline
import matplotlib
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import sum as spark_sum
import re
import pandas as pd



sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)



m = re.finditer(r'.*?(spark).*?', "I'm searching for a spark in PySpark", re.I)
for match in m:
    print(match, match.start(), match.end())

import glob

raw_data_files = glob.glob('/Users/jackpan/Downloads/*.gz')
base_df = spark.read.text(raw_data_files)
print(base_df.filter(base_df['value'].isNull()).count())
# base_df.printSchema()
#
# base_df.show(10, truncate=False)
# print((base_df.count(), len(base_df.columns)))

sample_logs = [item['value'] for item in base_df.take(15)]
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
hosts = [re.search(host_pattern, item).group(1)
         if re.search(host_pattern, item)
         else 'no match'
         for item in sample_logs]

print(hosts)

ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
timestamps = [re.search(ts_pattern, item).group(1) for item in sample_logs]
print(timestamps)

method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
method_uri_protocol = [re.search(method_uri_protocol_pattern, item).groups()
                       if re.search(method_uri_protocol_pattern, item)
                       else 'no match'
                       for item in sample_logs]
print(method_uri_protocol)

status_pattern = r'\s(\d{3})\s'
status = [re.search(status_pattern, item).group(1) for item in sample_logs]
print(status)

content_size_pattern = r'\s(\d+)$'
content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]
print(content_size)


logs_df = base_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
# logs_df.show(10, truncate=True)
# print((logs_df.count(), len(logs_df.columns)))

# bad_rows_df = logs_df.filter(logs_df['host'].isNull()|
#                              logs_df['timestamp'].isNull() |
#                              logs_df['method'].isNull() |
#                              logs_df['endpoint'].isNull() |
#                              logs_df['status'].isNull() |
#                              logs_df['content_size'].isNull()|
#                              logs_df['protocol'].isNull())
# print(bad_rows_df.count())
#
def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)
#
# # Build up a list of column expressions, one per column.
# exprs = [count_null(col_name) for col_name in logs_df.columns]
#
# # Run the aggregation. The *exprs converts the list of expressions into
# # variable function arguments.
# logs_df.agg(*exprs).show()

null_status_df = base_df.filter(~base_df['value'].rlike(r'\s(\d{3})\s'))
print(null_status_df.count())
null_status_df.show(truncate=False)
bad_status_df = null_status_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                      regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                                      regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                                      regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                                      regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                                      regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                                      regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
bad_status_df.show(truncate=False)

logs_df = logs_df[logs_df['status'].isNotNull()]
logs_df = logs_df.na.fill({'content_size': 0})
exprs = [count_null(col_name) for col_name in logs_df.columns]
logs_df.agg(*exprs).show()
from pyspark.sql.functions import udf
month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(text):
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
        int(text[7:11]),
        month_map[text[3:6]],
        int(text[0:2]),
        int(text[12:14]),
        int(text[15:17]),
        int(text[18:20])
    )

udf_parse_time = udf(parse_clf_time)
logs_df = (logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp'))

logs_df.show(10, truncate=True)
logs_df.cache()

# null_content_size_df = base_df.filter(~base_df['value'].rlike(r'\s\d+$'))
# print(null_content_size_df.count())
# print(null_content_size_df.take(10))

# content_size_summary_df = logs_df.describe(['content_size'])
# print(content_size_summary_df.toPandas())

from pyspark.sql import functions as F

# print(logs_df.agg(F.min(logs_df['content_size']).alias('min_content_size'),
#              F.max(logs_df['content_size']).alias('max_content_size'),
#              F.mean(logs_df['content_size']).alias('mean_content_size'),
#              F.stddev(logs_df['content_size']).alias('std_content_size'),
#              F.count(logs_df['content_size']).alias('count_content_size'))
#         .toPandas())

status_freq_df = (logs_df
                     .groupBy('status')
                     .count()
                     .sort('status')
                     .cache())
print('Total distinct HTTP Status Codes:', status_freq_df.count())

status_freq_pd_df = (status_freq_df.toPandas().sort_values(by=['count'], ascending=False))
print(status_freq_pd_df)

# import matplotlib.pyplot as plt
# import seaborn as sns
# import numpy as np
# sns.catplot(x='status', y='count', data=status_freq_pd_df,
#             kind='bar', order=status_freq_pd_df['status'])
# plt.savefig( 'myfig.png' )

host_sum_df = (logs_df.groupBy('host').count().sort('count', ascending=False).limit(10))
host_sum_df.show(truncate=False)






# Databricks notebook source
# MAGIC %md # Extract-Transform-Load
# MAGIC 
# MAGIC The **extract, transform, load (ETL)** process takes data from one (or more) source(s) e.g. an Amazon S3 bucket, transforms it, normally by adding structure, and then loads it into another data source e.g. a Database.
# MAGIC 
# MAGIC <img src="https://www.evernote.com/l/AAFVCRqZsmtPQqDx7JhUSRbHkauVzNHNeaQB/image.png" width=600px>
# MAGIC 
# MAGIC A common ETL job takes log files from a web server, parses out pertinent fields so it can be readily queried, and then loads it into a database.
# MAGIC 
# MAGIC ETL may seem simple: applying structure to data so it’s in a desired form. However, the complexity of ETL is in the details. Data Engineers building ETL pipelines must understand and apply the following concepts:<br><br>
# MAGIC 
# MAGIC * Optimizing data formats and connections
# MAGIC * Determining the ideal schema
# MAGIC * Handling corrupt records
# MAGIC * Automating workloads

# COMMAND ----------

# MAGIC %md ## ETL with Spark
# MAGIC 
# MAGIC Spark is a tool that can be used to interface with nearly any kind of data store. Spark provides a simple framework for distributed or parallel processing. For this reason, Spark applications can be easily scaled by simply adding more commodity hardware e.g AWS EC2 instances to a system.
# MAGIC 
# MAGIC Spark is
# MAGIC 
# MAGIC - **unified**
# MAGIC    - it can perform most of the tasks associated with data processing from ingestion to modeling
# MAGIC - **scalable**
# MAGIC    - code written for small applications can be reused in larger settings often by simply provisioning a larger cluster
# MAGIC - **flexible**
# MAGIC    - Spark decouples compute and storage
# MAGIC    - Spark can connect to most modern data store
# MAGIC    - with most Spark systems, you only pay for what you use

# COMMAND ----------

# MAGIC %md ### Credentials
# MAGIC 
# MAGIC To work on an ETL Pipeline, let's begin by configuring the simple system above.

# COMMAND ----------

# MAGIC %run ./includes/bdsetup

# COMMAND ----------

# MAGIC %md ## S3 Connection
# MAGIC 
# MAGIC This setup file connects Databricks to AWS S3. This is done by mounting an [S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html) to the [Databricks File System (DBFS)](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html#dbfs).
# MAGIC 
# MAGIC The Databricks Documentation provides [detailed instructions](https://docs.databricks.com/spark/latest/data-sources/aws/amazon-s3.html#mount-an-s3-bucket) on doing this.
# MAGIC 
# MAGIC The mounting is done by using the `dbutils.fs.mount` tool.

# COMMAND ----------

# MAGIC %fs ls /mnt

# COMMAND ----------

# MAGIC %md The Databricks File System Tools can be used to interact with the file system including mounting an S3 bucket as we just did.

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %md We can use `dbutils.fs.ls` to display the contents of the mount.

# COMMAND ----------

display(dbutils.fs.ls("/mnt/hadoop-and-big-data/nasa"))

# COMMAND ----------

# MAGIC %md If successful, you should see two files in the `nasa` directory.
# MAGIC 
# MAGIC This is sample data set of Nasa Apache Web Logs.
# MAGIC 
# MAGIC ### NASA Apache Web Logs
# MAGIC 
# MAGIC The sample time-series dataset comes from public 1995 NASA Apache web logs. The file contains data in an Imhotep-friendly TSV format.
# MAGIC 
# MAGIC A Perl script was used to convert the Apache web log into the TSV format, extracting the following fields:
# MAGIC 
# MAGIC | column | description |
# MAGIC |:-|:-|
# MAGIC | `host` |	When possible, the hostname making the request. Uses the IP address if the hostname was unavailable. |
# MAGIC | `logname` |	Unused, always - |
# MAGIC | `time` |	In seconds, since 1970 |
# MAGIC | `method` |	HTTP method: GET, HEAD, or POST |
# MAGIC | `url` |	Requested path |
# MAGIC | `response` |	HTTP response code |
# MAGIC | `bytes` |	Number of bytes in the reply |
# MAGIC 
# MAGIC Here is an example line (or document) from the dataset:
# MAGIC 
# MAGIC ```
# MAGIC piweba3y.prodigy.com - 807301196 GET /shuttle/missions/missions.html 200 8677
# MAGIC ```
# MAGIC 
# MAGIC The timestamp `807301196` is the conversion of `01/Aug/1995:13:19:56 -0500` using Perl:
# MAGIC 
# MAGIC ```
# MAGIC use Date::Parse;
# MAGIC $in = "01/Aug/1995:13:19:56 -0500";
# MAGIC $out = str2time($in);
# MAGIC print "$out\n";
# MAGIC ```

# COMMAND ----------

# MAGIC %md # Performing ETL - Extract Data
# MAGIC 
# MAGIC The files have the following numbers of records:
# MAGIC 
# MAGIC | file | number of records |
# MAGIC |:-|:-:|
# MAGIC | dbfs:/mnt/hadoop-and-big-data/nasa/nasa_19950630.22-19950728.12.tsv | 1891709 |
# MAGIC | dbfs:/mnt/hadoop-and-big-data/nasa/nasa_19950731.22-19950831.22.tsv | 1569886 |

# COMMAND ----------

# MAGIC %md We can use ``%fs head` to display the beginning of one of the files.

# COMMAND ----------

# MAGIC %fs head /mnt/hadoop-and-big-data/nasa/nasa_19950630.22-19950728.12.tsv

# COMMAND ----------

# MAGIC %md Here, we define the path to one of the files. We then use `spark.read.csv` to read in the file. Finally, we use `display` to view the resulting `DataFrame`.

# COMMAND ----------

log_1_path = "/mnt/hadoop-and-big-data/nasa/nasa_19950630.22-19950728.12.tsv"
nasa_log_1_df = (spark.read.csv(log_1_path))
display(nasa_log_1_df)

# COMMAND ----------

# MAGIC %md Note that the results don't look quite right. For one, the first row of data looks like a header. We can fix this by specifying `.option("header", True)` when we do the read.

# COMMAND ----------

nasa_log_1_df = (spark.read
                 .option("header", True)
                 .csv(log_1_path))
display(nasa_log_1_df)

# COMMAND ----------

# MAGIC %md Better, but still not quite right. This is because tabs (`\t`) are used to separate the data rather than commas.  We can fix this by specifying `.option("sep", "\t")` when we do the read.

# COMMAND ----------

nasa_log_1_df = (spark.read
                 .option("header", True)
                 .option("sep", "\t")
                 .csv(log_1_path))
display(nasa_log_1_df)

# COMMAND ----------

# MAGIC %md ## VALIDATION: Verify Number of Records
# MAGIC 
# MAGIC Does this cell match what we know about the file?

# COMMAND ----------

nasa_log_1_df.count()

# COMMAND ----------

# MAGIC %md ## What is a Spark DataFrame?
# MAGIC 
# MAGIC This code may help you to think about this question. First, we convert 500 rows of the Spark DataFrame into a Pandas DataFrame. Then we use `sys.getsize` to display how much memort is used for each object, `nasa_sample_df`, the Pandas DataFrame and `nasa_log_1_df`, the Spark DataFrame.

# COMMAND ----------

import sys

nasa_sample_df = nasa_log_1_df.limit(500).toPandas()
sys.getsizeof(nasa_sample_df), sys.getsizeof(nasa_log_1_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC Note that the Pandas DataFrame (which is only 500 rows) uses almost 5000 times the memory. This is because a Pandas DataFrame is the data loaded into memory. A Spark DataFrame is **a set of instructions to retrieve the data and distribute it across our cluster of workers**.

# COMMAND ----------

# MAGIC %md #Performing ETL - Transform Data

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime, unix_timestamp

nasa_log_1_sample_df = nasa_log_1_df.sample(withReplacement=False, fraction=0.1, seed=3)

nasa_log_1_sample_parse_time_df = (nasa_log_1_sample_df
                                   .withColumn("unixtime", from_unixtime(col("time")))
                                   .withColumn("timestamp", unix_timestamp(col("unixtime")).cast("timestamp"))
                                   .withColumn("bytes", col("bytes").cast("integer"))
                                   .withColumn("response", col("response").cast("integer")))
display(nasa_log_1_sample_parse_time_df)
# unix_timestamp(df.TIME,'dd-MMM-yyyy HH:mm:ss.SSS z')

# COMMAND ----------

# MAGIC %md ## VALIDATION: Validate Transformed Data

# COMMAND ----------

from pyspark.sql.functions import col, hour, from_utc_timestamp

getRequestsDF = (nasa_log_1_sample_parse_time_df
  .filter((col("response") >= 200) & (col("response") < 300))
  .select("method", "url", "timestamp", hour("timestamp").alias("hour"))
)

display(getRequestsDF)

# COMMAND ----------

display(getRequestsDF.groupby("hour").count())

# COMMAND ----------

display(getRequestsDF
        .groupby("url")
        .count()
        .orderBy(col("count").desc()))

# COMMAND ----------

# MAGIC %md # Performing ETL - Load Data
# MAGIC 
# MAGIC The load here will be done to the Databricks File System. The Databricks File System (DBFS) is an HDFS-like interface to bulk data stores like Amazon's S3 and Azure's Blob storage service.

# COMMAND ----------

nasa_log_1_sample_df.write.mode("overwrite").parquet("/data/NASA")

# COMMAND ----------

display(dbutils.fs.ls("/data/NASA"))

# COMMAND ----------

# MAGIC %fs dbfs://data/NASA

# COMMAND ----------

# MAGIC %md ## VALIDATION: Verify Data Load

# COMMAND ----------

display(spark.read.parquet("/data/NASA"))

# COMMAND ----------

# MAGIC %md # Exercise
# MAGIC 
# MAGIC You have already **extracted** the data as `nasa_log_1_sample_parse_time_df`.
# MAGIC 
# MAGIC 1. **transform** the data by doing a query on the data to obtain the top 10 requested `url` values.
# MAGIC 2. **load** the result to `"/data/NASA-top-10"`

# COMMAND ----------

#TODO

# COMMAND ----------

#ANSWER
from pyspark.sql.functions import col

top_10 = nasa_log_1_sample_parse_time_df.groupBy("url").count().orderBy(col("count").desc()).limit(10)
top_10.write.parquet("/data/NASA-top-10")

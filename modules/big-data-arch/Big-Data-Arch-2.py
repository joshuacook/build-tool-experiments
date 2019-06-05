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

# MAGIC %md ## S3 Connection
# MAGIC
# MAGIC If you worked through `Big-Data-Arch-1` you should already have the S3 bucket mounted. If not, this will do the mount for you:

# COMMAND ----------

# MAGIC %run ./includes/bdsetup

# COMMAND ----------

# MAGIC %fs ls /mnt/hadoop-and-big-data

# COMMAND ----------

# MAGIC %md Next, we display the contents of the `householdPowerConsumption` directory. This data was written to S3 using Spark. Note that it consists of several files of various sizes.
# MAGIC
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/_SUCCESS`
# MAGIC    - This is written to the system upon a successful write to DBFS
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/_committed_5364151664511134865`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/_started_5364151664511134865`
# MAGIC    - These two files are written by the system as metadata signifying the start and commit of a spark write
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00000-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-41-1-c000.json`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00001-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-42-1-c000.json`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00002-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-43-1-c000.json`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00003-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-44-1-c000.json`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00004-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-45-1-c000.json`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00005-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-46-1-c000.json`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00006-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-47-1-c000.json`
# MAGIC - `dbfs:/mnt/hadoop-and-big-data/householdPowerConsumption/part-00007-tid-5364151664511134865-95c20ac7-f4b3-4ef4-9974-5c546a6880d3-48-1-c000.json`
# MAGIC    - This is the **data**!!

# COMMAND ----------

# MAGIC %fs ls /mnt/hadoop-and-big-data/householdPowerConsumption

# COMMAND ----------

# MAGIC %md ## TODO: Read in the data
# MAGIC
# MAGIC Use `spark.read.json` to read in the data from the directory `"/mnt/hadoop-and-big-data/householdPowerConsumption"`.

# COMMAND ----------

# TODO
powerConsumptionDF = spark.read.json(FILL-ME-IN)
display(powerConsumptionDF)

# COMMAND ----------

#ANSWER
powerConsumptionDF = spark.read.json("/mnt/hadoop-and-big-data/householdPowerConsumption")
display(powerConsumptionDF)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Individual household electric power consumption Data Set
# MAGIC
# MAGIC **Abstract**: Measurements of electric power consumption in one household with a one-minute sampling rate over a period of almost 4 years. Different electrical quantities and some sub-metering values are available.
# MAGIC
# MAGIC **Data Set Characteristics**: Multivariate, Time-Series
# MAGIC
# MAGIC **Number of Instances**: 2075259
# MAGIC
# MAGIC **Data Set Information**:
# MAGIC This archive contains 2075259 measurements gathered in a house located in Sceaux (7km of Paris, France) between December 2006 and November 2010 (47 months).
# MAGIC
# MAGIC **Notes**:
# MAGIC 1. (global_active_power*1000/60 - sub_metering_1 - sub_metering_2 - sub_metering_3) represents the active energy consumed every minute (in watt hour) in the household by electrical equipment not measured in sub-meterings 1, 2 and 3.
# MAGIC 2. The dataset contains some missing values in the measurements (nearly 1,25% of the rows). All calendar timestamps are present in the dataset but for some timestamps, the measurement values are missing: a missing value is represented by the absence of value between two consecutive semi-colon attribute separators. For instance, the dataset shows missing values on April 28, 2007.
# MAGIC
# MAGIC **Desired Schema**
# MAGIC
# MAGIC 1. `date` : Date in format yyyy-mm-dd
# MAGIC 2. `time` : time in format hh:mm:ss
# MAGIC 3. `global_active_power` : household global minute-averaged active power (in kilowatt)
# MAGIC 4. `global_reactive_power` : household global minute-averaged reactive power (in kilowatt)
# MAGIC 5. `voltage` : minute-averaged voltage (in volt)
# MAGIC 6. `global_intensity` : household global minute-averaged current intensity (in ampere)
# MAGIC 7. `sub_metering_1` : energy sub-metering No. 1 (in watt-hour of active energy). It corresponds to the kitchen, containing mainly a dishwasher, an oven and a microwave (hot plates are not electric but gas powered).
# MAGIC 8. `sub_metering_2` : energy sub-metering No. 2 (in watt-hour of active energy). It corresponds to the laundry room, containing a washing-machine, a tumble-drier, a refrigerator and a light.
# MAGIC 9. `sub_metering_3` : energy sub-metering No. 3 (in watt-hour of active energy). It corresponds to an electric water-heater and an air-conditioner.

# COMMAND ----------

# MAGIC %md ## TODO: Display the Schema of the DataFrame
# MAGIC
# MAGIC Use `.printSchema()` to display the schema of `powerConsumptionDF`.

# COMMAND ----------

#TODO

# COMMAND ----------

#ANSWER
powerConsumptionDF.printSchema()

# COMMAND ----------

# MAGIC %md Notice that the schema of the DataFrame does not match the **desired schema**.

# COMMAND ----------

# MAGIC %md # ETL Task - Transform the Data to Match the Desired Schema

# COMMAND ----------

# MAGIC %md ### TODO: Convert the column `time` to two columns, `date` and `time` with the following formats:
# MAGIC
# MAGIC - `date` : Date in format yyyy-mm-dd
# MAGIC - `time` : time in format hh:mm:ss
# MAGIC
# MAGIC The Answer below is partially completed for you.

# COMMAND ----------

#TODO
from pyspark.sql.functions import col, explode, from_unixtime, split

powerConsumptionTimeConvertedDF = (powerConsumptionDF
                                   .withColumn("time", from_unixtime(col("time")))
                                   .withColumn("date", split(col("time"), " ").getItem(0)))

display(powerConsumptionTimeConvertedDF)

# COMMAND ----------

#ANSWER
from pyspark.sql.functions import col, explode, from_unixtime, split

powerConsumptionTimeConvertedDF = (powerConsumptionDF
                                   .withColumn("time", from_unixtime(col("time")))
                                   .withColumn("date", split(col("time"), " ").getItem(0))
                                   .withColumn("time", split(col("time"), " ").getItem(1)))

display(powerConsumptionTimeConvertedDF)

# COMMAND ----------

# MAGIC %md ### VALIDATION

# COMMAND ----------

try:
  assert powerConsumptionTimeConvertedDF.take(1)[0].asDict()["time"] == "06:56:00"
  print("YAY!!! Your answer is correct.")
except AssertionError:
  print("Your answer is incorrect.")

# COMMAND ----------

# MAGIC %md ### TODO: Convert the column `global` to three columns - `global_active_power`, `global_intensity`, and `global_reactive_power`
# MAGIC
# MAGIC The answer is started for you.
# MAGIC
# MAGIC **NOTE** Don't forget to drop `global` when finished.

# COMMAND ----------

#TODO
powerConsumptionTimeGlobalConvertedDF = (powerConsumptionTimeConvertedDF
                                         .withColumn("global_active_power", col("global.active_power")))
display(powerConsumptionTimeGlobalConvertedDF)

# COMMAND ----------

#ANSWER
powerConsumptionTimeGlobalConvertedDF = (powerConsumptionTimeConvertedDF
                                         .withColumn("global_active_power", col("global.active_power"))
                                         .withColumn("global_intensity", col("global.intensity"))
                                         .withColumn("global_reactive_power", col("global.reactive_power"))
                                         .drop("global"))
display(powerConsumptionTimeGlobalConvertedDF)

# COMMAND ----------

# MAGIC %md ### VALIDATION

# COMMAND ----------

try:
  assert powerConsumptionTimeGlobalConvertedDF.columns == ['Voltage',
 'submetering',
 'time',
 'date',
 'global_active_power',
 'global_intensity',
 'global_reactive_power']
  print("YAY!!! Your answer is correct.")
except AssertionError:
  print("Your answer is incorrect.")

# COMMAND ----------

# MAGIC %md ### TODO: Convert the column `submetering` to three columns - `sub_metering_1`, `sub_metering_2`, and `sub_metering_3`

# COMMAND ----------

#TODO
powerConsumptionTimeGlobalSubmeteringConvertedDF = None

# COMMAND ----------

#ANSWER
powerConsumptionTimeGlobalSubmeteringConvertedDF = (powerConsumptionTimeGlobalConvertedDF
                                         .withColumn("submetering_1", col("submetering.submeter_1"))
                                         .withColumn("submetering_2", col("submetering.submeter_2"))
                                         .withColumn("submetering_3", col("submetering.submeter_3"))
                                         .drop("submetering"))
display(powerConsumptionTimeGlobalSubmeteringConvertedDF)

# COMMAND ----------

# MAGIC %md ### VALIDATION

# COMMAND ----------

powerConsumptionTimeGlobalSubmeteringConvertedDF.columns

# COMMAND ----------

try:
  assert powerConsumptionTimeGlobalSubmeteringConvertedDF.columns == ['Voltage',
 'time',
 'date',
 'global_active_power',
 'global_intensity',
 'global_reactive_power',
 'submetering_1',
 'submetering_2',
 'submetering_3']
  print("YAY!!! Your answer is correct.")
except AssertionError:
  print("Your answer is incorrect.")

# COMMAND ----------

# MAGIC %md ## Postgres Connection
# MAGIC
# MAGIC Next, we will connect to a Postgres Database. There is a Postgres Database Server running at `34.210.22.0` on port `5433`. We will use the [Java Database Connectivity (JDBC)](https://www.oracle.com/technetwork/java/javase/jdbc/index.html) to connect to this database.
# MAGIC
# MAGIC First, let's define the database connection.

# COMMAND ----------

jdbcUrl = "jdbc:postgresql://{}:{}/{}".format(postgres_ip_address, postgres_port, "postgres")
postgresPropertis = {
  "user" : postgress_user,
  "password" : postgress_password
}

# COMMAND ----------

# MAGIC %md ## JOSHUA TODO: Write to Postgres

# COMMAND ----------

# MAGIC %md ## JOSHUA TODO: Write to S3

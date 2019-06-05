# Databricks notebook source
mongo_ip_address = "54.200.58.173"
mongo_port = 27017

postgres_ip_address = "34.210.22.0"
postgres_port = 5433
postgress_user = "postgres"
postgress_password = "postgres"

hbd_s3_access_key = "AKIASKPMJLAIVMM34NVL"
hbd_s3_secret_key = "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"
hbd_ro_s3_access_key = "AKIASKPMJLAIT225QEEJ"
hbd_ro_s3_secret_key = "X71zma5YkkRAij8XF6ovU0fBFOA0dDillT1CpogJ"

# COMMAND ----------

ACCESS_KEY = hbd_s3_access_key
SECRET_KEY = hbd_s3_secret_key
ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
AWS_BUCKET_NAME = "hadoop-and-big-data"
MOUNT_NAME = "hadoop-and-big-data"
SOURCE = "s3a://{}:{}@{}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME)
MOUNT_POINT = "/mnt/" + MOUNT_NAME

if "/mnt/hadoop-and-big-data" not in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
  dbutils.fs.mount(SOURCE, MOUNT_POINT)
else:
  print("/mnt/hadoop-and-big-data already mounted")

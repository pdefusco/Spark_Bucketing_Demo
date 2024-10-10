# Spark Bucketing in Cloudera

## Objective

This article provides an introduction to the Spark Bucketing using interactive Sessions in Cloudera Data Engineering (CDE). The article contains a demo / tutorial that you can reproduce in order to test different bucketing options using PySpark.

## Abstract

CDP Data Engineering (CDE) is the only cloud-native service purpose-built for enterprise data engineering teams. Building on Apache Spark, Data Engineering is an all-inclusive data engineering toolset that enables orchestration automation with Apache Airflow, advanced pipeline monitoring, visual troubleshooting, and comprehensive management tools to streamline ETL processes across enterprise analytics teams.

Spark bucketing is an optimization technique used to enhance the performance of data processing by reducing the amount of data shuffled during join operations or aggregations. In bucketing, data is divided into a fixed number of buckets based on the value of one or more columns, ensuring that rows with the same bucket key are grouped together in the same bucket. This structured partitioning allows Spark to reduce shuffle operations and improve query performance, especially when performing joins between two bucketed tables.

## Requirements

* CDE Virtual Cluster of type "All-Purpose" running in CDE Service with version 1.22 or above, and Spark version 3.2 or above.
* CDE Docker Runtime Entitlement.
* An installation of the CDE CLI is recommended but optional. In the steps below you will create the CDE Session using the CLI, but you can alternatively launch one using the UI.
* Optional: if you choose to run the demo in CML, you can run the same code using cml/demo.py. However, notice that the data needs to be initially created using CDE following instructions in the "Setup & Data Gen" section below.

## Step by Step Guide

Create a CDE Docker Resource in order to provide dependencies for the Data Generation job.

#### Setup & Data Gen

```
### Setup

cde credential create \
  --name dckr-crds \
  --type docker-basic \
  --docker-server hub.docker.com \
  --docker-username pauldefusco

cde resource create \
  --name datagen-runtime \
  --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-dbldatagen-002 \
  --image-engine spark3 \
  --type custom-runtime-image

cde resource create \
  --name files-spark32 \
  --type files

cde resource upload \
  --name files-spark32 \
  --local-path jobs/datagen.py
```

Generate data in CDE with a Spark Job.

```
### Data Gen Job

cde job delete \
  --name datagen_10M

cde job create \
  --name datagen_10M \
  --arg 25 \
  --arg 25 \
  --arg 10000000 \
  --arg db_10M \
  --arg source_10M \
  --type spark \
  --mount-1-resource files-spark32 \
  --application-file datagen.py \
  --runtime-image-resource-name datagen-runtime \
  --executor-cores 4 \
  --executor-memory "4g"

cde job run \
  --name datagen_10M \
  --executor-memory "10g"

cde session create \
  --name spark_bucketing \
  --type pyspark \
  --executor-cores 5 \
  --executor-memory "10g" \
  --num-executors 5

cde session create \
  --name spark_bucketing_test \
  --type pyspark \
  --executor-cores 5 \
  --executor-memory "10g" \
  --num-executors 5 \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=20
```

#### CDE Session Commands

Familiarize yourself with the data:

```
# Session Commands:

db_name = "db_10M"
src_tbl = "source_10M"
tgt_tbl = "target_10M"
buckets = 25

spark.sql("drop table if exists {0}.{1}".format(db_name, tgt_tbl))
df = spark.sql("select * from {0}.{1}".format(db_name, src_tbl))

# Spark SQL Command:
print(spark.sql("SHOW CREATE TABLE {0}.{1}".format(db_name, src_tbl)).collect()[0][0])

# Expected Output:
CREATE TABLE `db_10M`.`source_10M` (
  `unique_id` INT,
  `code` STRING,
  `col1` FLOAT,
  `col2` FLOAT,
  .
  .
  .
  `col99` FLOAT)
USING parquet
LOCATION 's3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/db_10m.db/source_10m'
TBLPROPERTIES (
  'numFilesErasureCoded' = '0',
  'bucketing_version' = '2',
  'TRANSLATED_TO_EXTERNAL' = 'TRUE',
  'external.table.purge' = 'TRUE')
```

Now explore bucketing behavior with a series of Examples.

##### Example 1: Repartition the source table into a single partition, then bucket and sort by "unique_id"

This produced 25 evenly sized files of 64 MB each corresponding to 25 buckets. Each row is assigned into a bucket by means of a hashing function. On disk, the data will be written into files named "part-0000" but with 25 separate bucket ID's.

```
## PySpark Code in Session:

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 2: Repartition the source table into 25 partitions, then bucket and sort by "unique_id".

* This produces 25 partitions, where within each partition each row is assigned to a bucket by means of a hashing function.
* Notice we are not asking spark to repartition by a specific column. Spark just uses internal statistics to produce an as even as possible distribution of data among partitions.
* Depending on how data is distributed among each of the 25 partitions i.e. how skewed it is, Spark will hash each row based on "unique_id" and create bucket files within each partition.
* The max number of possible files created is 625 for perfectly evenly distributed data. In this case, each file is roughly 6MB.

```
## PySpark Code in Session:

df.repartition(25) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 3: Repartition the source table based on a column with low cardinality and even distribution (i.e. low skew) and then bucket and sort with "unique_id".

* Generally the best approache among the examples so far, especially at larger scale. However, it requires identifying a column that has relatively low cardinality and even distribution.
* In this case this is "code"  which uniformly takes a value among a list of 9 string codes. But I know this because I generate the synthetic data. In your case you'd have to do some tests with your data.
This creates a maximum of 9 partitions x 25 buckets (225), reflecting the number of unique code categories and requested buckets.
* Notice ther heavy data skew between the different values.

Explore cardinality of the "Code" column:

```
## PySpark Code in Session:

spark.sql("""SELECT CODE, COUNT(*)
            FROM {0}.{1}
            GROUP BY CODE""".format(db_name, src_tbl)).show()

## Expected Output:
+----+--------+
|CODE|count(1)|
+----+--------+
|   g| 1819875|
|   f| 1817273|
|   e|  909405|
|   h| 1363761|
|   d|  911224|
|   c|  909449|
|   i|  907558|
|   b|  907035|
|   a|  454420|
+----+--------+
```

Now bucket your data by "unique_id", but repartition by "code":

```
df.repartition("code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)
```

##### Example 4: Repartition by evenly distributed column with low cardinality, but reduce number of partitions even further

* Because the number of files can reach the product of the number of partitions and the number of buckets, "bucketBy" can be very slow for large datasets where partitions are randomly arranged with respect to bucketing columns.
* Repartitioning can be dramatically faster when the number of partitions is set equal to the number of buckets, and the repartitioning key is made up of the bucketing columns. In other words, you will get the lowest number of files when you can align buckets and partitions i.e. having the repartitoning key made up of bucketing column(s).
* However, this is hard to achieve because the data may not lend itself to this. Regardless, testing different combinations of repartitioning and bucketing keys with the objective of getting to an as small as possible number of files, aiming at roughly 128 MB filesize, is an exercise that goes in the direction of this "ideal" scenario.  
* In the below example, the data is repartitioned by the low cardinality "code" column, as before, but this time the number of partitions is reduced to 5 resulting in 125 files i.e. a lower number of files each with more data than the previous example 3. Notice that the data skew in the "code" column leads to file size differences, with many files reaching ~40MB while others ~20MB or even as low as 7 MB in size.

```
# PySpark Code

df.repartition(5, "code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name+"." + tgt_tbl)
```

## Summary

Cloudera Data Engineering (CDE) and the broader Cloudera Data Platform (CDP) offer a powerful, scalable solution for building, deploying, and managing data workflows in hybrid and multi-cloud environments. CDE simplifies data engineering with serverless architecture, auto-scaling Spark clusters, and built-in Apache Iceberg support.

You can use CDE Sessions to prototype your Spark Applications in PySpark, Scala, and Java, as you did with the above Bucketing examples. Spark bucketing is an optimization technique used to enhance the performance of data processing by reducing the amount of data shuffled during join operations or aggregations. Spark Bucketing is particularly useful when dealing with large datasets, as it enables more efficient data processing and resource utilization.

## Next Steps

Here is a list of helpful articles and blogs related to Cloudera Data Engineering and Apache Iceberg:

- **Cloudera on Public Cloud 5-Day Free Trial**
   Experience Cloudera Data Engineering through common use cases that also introduce you to the platform’s fundamentals and key capabilities with predefined code samples and detailed step by step instructions.
   [Try Cloudera on Public Cloud for free](https://www.cloudera.com/products/cloudera-public-cloud-trial.html?utm_medium=sem&utm_source=google&keyplay=ALL&utm_campaign=FY25-Q2-GLOBAL-ME-PaidSearch-5-Day-Trial%20&cid=701Hr000001fVx4IAE&gad_source=1&gclid=EAIaIQobChMI4JnvtNHciAMVpAatBh2xRgugEAAYASAAEgLke_D_BwE)

- **Cloudera Blog: Supercharge Your Data Lakehouse with Apache Iceberg**  
   Learn how Apache Iceberg integrates with Cloudera Data Platform (CDP) to enable scalable and performant data lakehouse solutions, covering features like in-place table evolution and time travel.  
   [Read more on Cloudera Blog](https://blog.cloudera.com/supercharge-your-data-lakehouse-with-apache-iceberg-in-cloudera-data-platform/)

- **Compatibility for Cloudera Data Engineering and Runtime Components**
  Learn about Cloudera Data Engineering (CDE) and compatibility for Runtime components across different versions. This document also includes component version compatibility information for AWS Graviton.
  [REad more in the Cloudera Documentation](https://docs.cloudera.com/data-engineering/cloud/release-notes/topics/cde-dl-compatibility.html)

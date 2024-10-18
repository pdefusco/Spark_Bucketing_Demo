#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '5')
SparkContext.setSystemProperty('spark.executor.memory', '10g')

# UPDATE CONNECTION NAME
CONNECTION_NAME = "paul-aug26-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

# UPDATE DB AND TABLE NAMES
db_name = "db_10M"
src_tbl = "source_10M"
tgt_tbl = "target_10M"
buckets = 25

spark.sql("drop table if exists {0}.{1}".format(db_name, tgt_tbl))
df = spark.sql("select * from {0}.{1}".format(db_name, src_tbl))

# Spark SQL Command:
print(spark.sql("SHOW CREATE TABLE {0}.{1}".format(db_name, src_tbl).collect()[0][0])

## Example 1

## PySpark Code in Session:

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)

## Example 2

## PySpark Code in Session:

df.repartition(25) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)

## Example 3

## PySpark Code in Session:

spark.sql("""SELECT CODE, COUNT(*)
            FROM {0}.{1}
            GROUP BY CODE""".format(db_name, src_tbl)).show()

df.repartition("code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)

## Example 4

# PySpark Code

df.repartition(5, "code") \
  .write \
  .mode("overwrite") \
  .bucketBy(buckets, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + tgt_tbl)

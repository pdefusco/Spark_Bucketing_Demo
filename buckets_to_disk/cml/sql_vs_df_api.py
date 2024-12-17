#****************************************************************************
# (C) Cloudera, Inc. 2020-2024
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

import os
import numpy as np
import pandas as pd
from datetime import datetime
import dbldatagen as dg
import dbldatagen.distributions as dist
from dbldatagen import FakerTextFactory, DataGenerator, fakerText
from faker.providers import bank, credit_card, currency
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField

class DataGen:

    '''Class to Generate Banking Data'''

    def __init__(self, spark):
        self.spark = spark

    def df1Gen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 100000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("ak_user_id", "int", minValue=0, maxValue=100000, random=False)
                    .withColumn("level_code", "string", values=["0", "1", "2", "3"], random=True)

                    )

        df = fakerDataspec.build()

        return df

    def df2Gen(self, shuffle_partitions_requested = 10, partitions_requested = 10, data_rows = 100000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumn("ak_user_id", "int", minValue=0, maxValue=100000, random=False)
                    )

        df = fakerDataspec.build()

        return df


import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
#from pyspark import SparkContext
#SparkContext.setSystemProperty('spark.executor.cores', '1')
#SparkContext.setSystemProperty('spark.executor.memory', '2g')

CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

myDG = DataGen(spark)

df1 = myDG.df1Gen()
df2 = myDG.df2Gen()

spark.sql("DROP TABLE IF EXISTS large_xref_table")
spark.sql("DROP TABLE IF EXISTS input_trans_table")

df1.write.saveAsTable("large_xref_table")
df2.write.saveAsTable("input_trans_table")

spark.sql("DROP TABLE IF EXISTS table_a")
spark.sql("DROP TABLE IF EXISTS table_b")

# SPARK SQL API

spark.sql("""
CREATE TABLE table_a
    ( ak_user_id BIGINT, level_code STRING)
    USING parquet
    CLUSTERED BY (ak_user_id) INTO 20 BUCKETS
""")

spark.sql("""
CREATE TABLE table_b
    ( ak_user_id BIGINT)
    USING parquet
    CLUSTERED BY (ak_user_id) INTO 20 BUCKETS
""")

spark.sql("""
INSERT INTO table_a FROM large_xref_table SELECT ak_user_id, level_code
""")

spark.sql("""
INSERT INTO table_b FROM input_trans_table SELECT ak_user_id
""")

spark.sql("""
CREATE TABLE table_c_match
    USING parquet
    CLUSTERED BY ( ak_user_id ) INTO 20 buckets
    AS SELECT table_b.ak_user_id, table_a.level_code FROM table_a LEFT JOIN table_b
    ON table_a.ak_user_id = table_b.ak_user_id
""")

# DATAFRAME API

myDG = DataGen(spark)

df1 = myDG.df1Gen()
df2 = myDG.df2Gen()

spark.sql("DROP TABLE IF EXISTS large_xref_table_2")
spark.sql("DROP TABLE IF EXISTS input_trans_table_2")
spark.sql("DROP TABLE IF EXISTS table_c_match_2")

df1.repartition(1).write.mode("overwrite").bucketBy(20, "ak_user_id").saveAsTable("large_xref_table_2")
df2.repartition(1).write.mode("overwrite").bucketBy(20, "ak_user_id").saveAsTable("input_trans_table_2")

df1 = spark.sql("SELECT * FROM large_xref_table_2")
df2 = spark.sql("SELECT * FROM input_trans_table_2")

df_c_match = df1.join(df2, df1["ak_user_id"] == df2["ak_user_id"], "left_outer").drop(df1["ak_user_id"])

df_c_match.repartition(1).write.mode("overwrite").bucketBy(5, "ak_user_id").saveAsTable("table_c_match_2")

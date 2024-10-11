import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '5')
SparkContext.setSystemProperty('spark.executor.memory', '10g')

CONNECTION_NAME = "paul-aug26-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()

db_name = "db_10M"
src_tbl = "source_10M"
join_tbl_left = "join_10M_left"
join_tbl_right = "join_10M_right"
buckets = 25

df = spark.sql("select * from {0}.{1}".format(db_name, src_tbl))



## Example 1: Create Evenly Bucketed Tables - Both Tables Bucketed by Unique ID:

## PySpark Code in Session:

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + join_tbl_left)

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + join_tbl_right)

## Read Bucketed Tables

df_left = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_left))
df_right = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_right))

## Join with Bucketed Tables
import time
start_time = time.time()

df_join = df_left.join(
  df_right,
  on="unique_id",
  how="inner"
)

df_join.explain()

"""
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [unique_id#1720, code#1721, col1#1722, col2#1723, col3#1724, col4#1725, col5#1726, col6#1727, col7#1728, col8#1729, col9#1730, col10#1731, col11#1732, col12#1733, col13#1734, col14#1735, col15#1736, col16#1737, col17#1738, col18#1739, col19#1740, col20#1741, col21#1742, col22#1743, ... 177 more fields]
   +- SortMergeJoin [unique_id#1720], [unique_id#1923], Inner
      :- Sort [unique_id#1720 ASC NULLS FIRST], false, 0
      :  +- Filter isnotnull(unique_id#1720)
      :     +- FileScan parquet db_10m.join_10m_left[unique_id#1720,code#1721,col1#1722,col2#1723,col3#1724,col4#1725,col5#1726,col6#1727,col7#1728,col8#1729,col9#1730,col10#1731,col11#1732,col12#1733,col13#1734,col14#1735,col15#1736,col16#1737,col17#1738,col18#1739,col19#1740,col20#1741,col21#1742,col22#1743,... 77 more fields] Batched: false, Bucketed: true, DataFilters: [isnotnull(unique_id#1720)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa..., SelectedBucketsCount: 25 out of 25
      +- Sort [unique_id#1923 ASC NULLS FIRST], false, 0
         +- Filter isnotnull(unique_id#1923)
            +- FileScan parquet db_10m.join_10m_right[unique_id#1923,code#1924,col1#1925,col2#1926,col3#1927,col4#1928,col5#1929,col6#1930,col7#1931,col8#1932,col9#1933,col10#1934,col11#1935,col12#1936,col13#1937,col14#1938,col15#1939,col16#1940,col17#1941,col18#1942,col19#1943,col20#1944,col21#1945,col22#1946,... 77 more fields] Batched: false, Bucketed: true, DataFilters: [isnotnull(unique_id#1923)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa..., SelectedBucketsCount: 25 out of 25
"""

print(f"time taken for evenly bucketed join: {time.time() - start_time}")
"""
time taken for evenly bucketed join: 0.18408608436584473
"""



## Example 2: Create Unevenly Bucketed Tables - One Table is Partitioned by Code, the other Bucketed by Unique ID:

## PySpark Code in Session:


df.write \
  .mode("overwrite") \
  .partitionBy("code") \
  .saveAsTable(db_name + "." + join_tbl_left)

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(25, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + join_tbl_right)

## Read Bucketed Tables

df_left = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_left))
df_right = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_right))

## Join with Bucketed Tables

start_time = time.time()

df_join = df_left.join(
  df_right,
  on="unique_id",
  how="inner"
)

df_join.explain()
"""
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [unique_id#2833, col1#2834, col2#2835, col3#2836, col4#2837, col5#2838, col6#2839, col7#2840, col8#2841, col9#2842, col10#2843, col11#2844, col12#2845, col13#2846, col14#2847, col15#2848, col16#2849, col17#2850, col18#2851, col19#2852, col20#2853, col21#2854, col22#2855, col23#2856, ... 177 more fields]
   +- SortMergeJoin [unique_id#2833], [unique_id#3036], Inner
      :- Sort [unique_id#2833 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(unique_id#2833, 25), ENSURE_REQUIREMENTS, [id=#164]
      :     +- Filter isnotnull(unique_id#2833)
      :        +- FileScan parquet db_10m.join_10m_left[unique_id#2833,col1#2834,col2#2835,col3#2836,col4#2837,col5#2838,col6#2839,col7#2840,col8#2841,col9#2842,col10#2843,col11#2844,col12#2845,col13#2846,col14#2847,col15#2848,col16#2849,col17#2850,col18#2851,col19#2852,col20#2853,col21#2854,col22#2855,col23#2856,... 77 more fields] Batched: false, DataFilters: [isnotnull(unique_id#2833)], Format: Parquet, Location: CatalogFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/d..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,col1:float,col2:float,col3:float,col4:float,col5:float,col6:float,col7:float...
      +- Sort [unique_id#3036 ASC NULLS FIRST], false, 0
         +- Filter isnotnull(unique_id#3036)
            +- FileScan parquet db_10m.join_10m_right[unique_id#3036,code#3037,col1#3038,col2#3039,col3#3040,col4#3041,col5#3042,col6#3043,col7#3044,col8#3045,col9#3046,col10#3047,col11#3048,col12#3049,col13#3050,col14#3051,col15#3052,col16#3053,col17#3054,col18#3055,col19#3056,col20#3057,col21#3058,col22#3059,... 77 more fields] Batched: false, Bucketed: true, DataFilters: [isnotnull(unique_id#3036)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa..., SelectedBucketsCount: 25 out of 25
"""

print(f"time taken: {time.time() - start_time}")

"""
time taken: 0.07133173942565918
"""

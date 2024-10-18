import time
import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '5')
SparkContext.setSystemProperty('spark.executor.memory', '2g')

CONNECTION_NAME = "paul-aug26-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

db_name = "db_1M"
src_tbl = "source_1M"
join_tbl_left = "join_1M_left"
join_tbl_right = "join_1M_right"

df = spark.sql("SELECT * FROM {0}.{1}".format(db_name, src_tbl))
print(spark.sql("SHOW CREATE TABLE {0}.{1}".format(db_name, src_tbl)).collect()[0][0])
spark.sql("SELECT code, COUNT(*) FROM {0}.{1} GROUP BY code".format(db_name, src_tbl)).show()


## Example 1: Create Evenly Bucketed Tables - Both Tables Bucketed by Unique ID:

## PySpark Code in Session:

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(5, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + join_tbl_left)

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(5, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + join_tbl_right)

## Read Bucketed Tables

df_left = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_left))
df_right = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_right))

## Join with Bucketed Tables

df_join = df_left.join(
  df_right,
  on="unique_id",
  how="inner"
)

df_join.explain()
"""
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [unique_id#725, code#726, col1#727, col2#728, col3#729, col4#730, col5#731, col6#732, col7#733, col8#734, col9#735, col10#736, col11#737, col12#738, col13#739, col14#740, col15#741, col16#742, col17#743, col18#744, col19#745, col20#746, col21#747, col22#748, ... 177 more fields]
   +- SortMergeJoin [unique_id#725], [unique_id#928], Inner
      :- Sort [unique_id#725 ASC NULLS FIRST], false, 0
      :  +- Filter isnotnull(unique_id#725)
      :     +- FileScan parquet db_1m.join_1m_left[unique_id#725,code#726,col1#727,col2#728,col3#729,col4#730,col5#731,col6#732,col7#733,col8#734,col9#735,col10#736,col11#737,col12#738,col13#739,col14#740,col15#741,col16#742,col17#743,col18#744,col19#745,col20#746,col21#747,col22#748,... 77 more fields] Batched: false, Bucketed: true, DataFilters: [isnotnull(unique_id#725)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa..., SelectedBucketsCount: 5 out of 5
      +- Sort [unique_id#928 ASC NULLS FIRST], false, 0
         +- Filter isnotnull(unique_id#928)
            +- FileScan parquet db_1m.join_1m_right[unique_id#928,code#929,col1#930,col2#931,col3#932,col4#933,col5#934,col6#935,col7#936,col8#937,col9#938,col10#939,col11#940,col12#941,col13#942,col14#943,col15#944,col16#945,col17#946,col18#947,col19#948,col20#949,col21#950,col22#951,... 77 more fields] Batched: false, Bucketed: true, DataFilters: [isnotnull(unique_id#928)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa..., SelectedBucketsCount: 5 out of 5
"""

start_time = time.time()
df_join.count()
print(f"time taken for evenly bucketed join: {time.time() - start_time}")
#time taken for evenly bucketed join: 1.2943942546844482


## Example 2: One table is bucketed, the other table is not.

## PySpark Code in Session:

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(5, "unique_id") \
  .sortBy("unique_id") \
  .saveAsTable(db_name + "." + join_tbl_left)

df.repartition(1) \
  .write \
  .mode("overwrite") \
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
+- Project [unique_id#809, code#810, col1#811, col2#812, col3#813, col4#814, col5#815, col6#816, col7#817, col8#818, col9#819, col10#820, col11#821, col12#822, col13#823, col14#824, col15#825, col16#826, col17#827, col18#828, col19#829, col20#830, col21#831, col22#832, ... 177 more fields]
   +- SortMergeJoin [unique_id#809], [unique_id#1012], Inner
      :- Sort [unique_id#809 ASC NULLS FIRST], false, 0
      :  +- Filter isnotnull(unique_id#809)
      :     +- FileScan parquet db_1m.join_1m_left[unique_id#809,code#810,col1#811,col2#812,col3#813,col4#814,col5#815,col6#816,col7#817,col8#818,col9#819,col10#820,col11#821,col12#822,col13#823,col14#824,col15#825,col16#826,col17#827,col18#828,col19#829,col20#830,col21#831,col22#832,... 77 more fields] Batched: false, Bucketed: true, DataFilters: [isnotnull(unique_id#809)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa..., SelectedBucketsCount: 5 out of 5
      +- Sort [unique_id#1012 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(unique_id#1012, 5), ENSURE_REQUIREMENTS, [id=#65]
            +- Filter isnotnull(unique_id#1012)
               +- FileScan parquet db_1m.join_1m_right[unique_id#1012,code#1013,col1#1014,col2#1015,col3#1016,col4#1017,col5#1018,col6#1019,col7#1020,col8#1021,col9#1022,col10#1023,col11#1024,col12#1025,col13#1026,col14#1027,col15#1028,col16#1029,col17#1030,col18#1031,col19#1032,col20#1033,col21#1034,col22#1035,... 77 more fields] Batched: false, DataFilters: [isnotnull(unique_id#1012)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa...
"""

start_time = time.time()
df_join.count()
print(f"time taken for unevenly bucketed join: {time.time() - start_time}")
#time taken for unevenly bucketed join: 1.4885172843933105


## Example 3: Create Unevenly Bucketed Tables - One Table is non-partitioned, the other Bucketed by Unique ID, but the join is happening on a different column:

## PySpark Code in Session:

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(5, "col5") \
  .sortBy("col5") \
  .saveAsTable(db_name + "." + join_tbl_left)

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(5, "col6") \
  .sortBy("col6") \
  .saveAsTable(db_name + "." + join_tbl_right)

## Read Bucketed Tables

df_left = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_left))
df_right = spark.sql("SELECT * FROM {0}.{1}".format(db_name, join_tbl_right))

## Join

df_join = df_left.join(
  df_right,
  on="unique_id",
  how="inner"
)

df_join.explain()
"""
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- Project [unique_id#809, code#810, col1#811, col2#812, col3#813, col4#814, col5#815, col6#816, col7#817, col8#818, col9#819, col10#820, col11#821, col12#822, col13#823, col14#824, col15#825, col16#826, col17#827, col18#828, col19#829, col20#830, col21#831, col22#832, ... 177 more fields]
   +- SortMergeJoin [unique_id#809], [unique_id#1012], Inner
      :- Sort [unique_id#809 ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(unique_id#809, 200), ENSURE_REQUIREMENTS, [id=#70]
      :     +- Filter isnotnull(unique_id#809)
      :        +- FileScan parquet db_100k.join_100k_left[unique_id#809,code#810,col1#811,col2#812,col3#813,col4#814,col5#815,col6#816,col7#817,col8#818,col9#819,col10#820,col11#821,col12#822,col13#823,col14#824,col15#825,col16#826,col17#827,col18#828,col19#829,col20#830,col21#831,col22#832,... 77 more fields] Batched: false, Bucketed: false (disabled by query planner), DataFilters: [isnotnull(unique_id#809)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa...
      +- Sort [unique_id#1012 ASC NULLS FIRST], false, 0
         +- Exchange hashpartitioning(unique_id#1012, 200), ENSURE_REQUIREMENTS, [id=#63]
            +- Filter isnotnull(unique_id#1012)
               +- FileScan parquet db_100k.join_100k_right[unique_id#1012,code#1013,col1#1014,col2#1015,col3#1016,col4#1017,col5#1018,col6#1019,col7#1020,col8#1021,col9#1022,col10#1023,col11#1024,col12#1025,col13#1026,col14#1027,col15#1028,col16#1029,col17#1030,col18#1031,col19#1032,col20#1033,col21#1034,col22#1035,... 77 more fields] Batched: false, DataFilters: [isnotnull(unique_id#1012)], Format: Parquet, Location: InMemoryFileIndex(1 paths)[s3a://paul-aug26-buk-a3c2b50a/data/warehouse/tablespace/external/hive/..., PartitionFilters: [], PushedFilters: [IsNotNull(unique_id)], ReadSchema: struct<unique_id:int,code:string,col1:float,col2:float,col3:float,col4:float,col5:float,col6:floa...
"""

import time
start_time = time.time()
df_join.count()
print(f"time taken for unevenly bucketed join: {time.time() - start_time}")
#time taken for unevenly bucketed join: 2.6929166316986084



## Example 4: Bucket Pruning

## PySpark Code in Session:

bucket_tbl_left = "bucket_left_1M"
bucket_tbl_right = "bucket_right_1M"

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(5, "code") \
  .sortBy("code") \
  .saveAsTable(db_name + "." + bucket_tbl_left)

df.repartition(1) \
  .write \
  .mode("overwrite") \
  .bucketBy(10, "col6") \
  .sortBy("col6") \
  .saveAsTable(db_name + "." + bucket_tbl_right)

df_buck_prune_left = spark.sql("SELECT * FROM {0}.{1} WHERE code == 'g'".format(db_name, bucket_tbl_left))
df_buck_prune_right = spark.sql("SELECT * FROM {0}.{1} WHERE code == 'g'".format(db_name, bucket_tbl_right))

df_buck_prune_join = df_buck_prune_left.join(
  df_buck_prune_right,
  on="unique_id",
  how="inner"
)

df_buck_prune_join.explain()
"""
"""

import time
start_time = time.time()
df_buck_prune_join.count()
print(f"time taken for bucket pruning join: {time.time() - start_time}")
#time taken for evenly bucketed join: 1.9484400749206543

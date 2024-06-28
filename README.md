# CDE_Spark_Bucketing

```
cde credential create --name dckr-crds --type docker-basic --docker-server hub.docker.com --docker-username pauldefusco
cde resource create --name datagen-runtime --image pauldefusco/dex-spark-runtime-3.2.3-7.2.15.8:1.20.0-b15-dbldatagen-002 --image-engine spark3 --type custom-runtime-image

cde resource create --name files-spark32 --type files
cde resource upload --name files-spark32 --local-path jobs/datagen.py

cde job delete --name datagen_100M
cde job delete --name datagen_500M
cde job delete --name datagen_1B

cde job create --name datagen_100M --arg 10 --arg 10 --arg 100000000 --arg db_100M --arg source_100M --type spark --mount-1-resource files-spark32 --application-file datagen.py --runtime-image-resource-name datagen-runtime --executor-cores 4 --executor-memory "4g"

cde job create --name datagen_500M --arg 10 --arg 10 --arg 500000000 --arg db_500M --arg source_500M --type spark --mount-1-resource files-spark32 --application-file datagen.py --runtime-image-resource-name datagen-runtime --executor-cores 4 --executor-memory "4g"

cde job create --name datagen_1B --arg 10 --arg 10 --arg 1000000000 --arg db_1B --arg source_1B --type spark --mount-1-resource files-spark32 --application-file datagen.py --runtime-image-resource-name datagen-runtime --executor-cores 4 --executor-memory "4g"

cde job run --name datagen_100M
cde job run --name datagen_500M
cde job run --name datagen_1B

cde resource upload --name files-spark32 --local-path jobs/etljob_random.py --local-path jobs/etljob_uniform.py

cde job delete --name etljob_100M_random
cde job delete --name etljob_500M_random
cde job delete --name etljob_1B_random
cde job delete --name etljob_100M_uniform
cde job delete --name etljob_500M_uniform
cde job delete --name etljob_1B_uniform

cde job create --name etljob_100M_random --application-file etljob_random.py --arg db_100M --arg source_100M --arg tgt_100M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_500M_random --application-file etljob_random.py --arg db_500M --arg source_500M --arg tgt_500M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_1B_random --application-file etljob_random.py --arg db_1B --arg source_1B --arg tgt_1B --arg 25 --type spark --mount-1-resource files-spark32

cde job run --name etljob_100M_random --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_500M_random --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_1B_random --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10

cde job create --name etljob_100M_uniform --application-file etljob_uniform.py --arg db_100M --arg source_100M --arg tgt_100M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_500M_uniform --application-file etljob_uniform.py --arg db_500M --arg source_500M --arg tgt_500M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_1B_uniform --application-file etljob_uniform.py --arg db_1B --arg source_1B --arg tgt_1B --arg 25 --type spark --mount-1-resource files-spark32

cde job run --name etljob_100M_uniform --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_500M_uniform --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_1B_uniform --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10

cde resource upload --name files-spark32 --local-path jobs/etljob_repartition_random.py --local-path jobs/etljob_repartition_uniform.py

cde job create --name etljob_100M_repartition_random --application-file etljob_repartition_random.py --arg db_100M --arg source_100M --arg tgt_100M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_500M_repartition_random --application-file etljob_repartition_random.py --arg db_500M --arg source_500M --arg tgt_500M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_1B_repartition_random --application-file etljob_repartition_random.py --arg db_1B --arg source_1B --arg tgt_1B --arg 25 --type spark --mount-1-resource files-spark32

cde job run --name etljob_100M_repartition_random --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_500M_repartition_random --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_1B_repartition_random --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10

cde job create --name etljob_100M_repartition_uniform --application-file etljob_repartition_uniform.py --arg db_100M --arg source_100M --arg tgt_100M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_500M_repartition_uniform --application-file etljob_repartition_uniform.py --arg db_500M --arg source_500M --arg tgt_500M --arg 25 --type spark --mount-1-resource files-spark32
cde job create --name etljob_1B_repartition_uniform --application-file etljob_repartition_uniform.py --arg db_1B --arg source_1B --arg tgt_1B --arg 25 --type spark --mount-1-resource files-spark32

cde job run --name etljob_100M_repartition_uniform --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_500M_repartition_uniform --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10
cde job run --name etljob_1B_repartition_uniform --executor-cores 2 --executor-memory "4g" --min-executors 1 --max-executors 10

```

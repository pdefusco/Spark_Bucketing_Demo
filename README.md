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

## Contents

* Writing Bucketed Datasets to Disk
* Improving Join Performance with Bucketing
* Bucketing and Iceberg

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

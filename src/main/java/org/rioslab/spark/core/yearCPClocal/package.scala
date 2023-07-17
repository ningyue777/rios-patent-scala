package org.rioslab.spark.core.yearCPClocal

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object yearCPClocalobject {
  def run(args: Array[String]): String = {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Yearly CPC Application")
    val spark = SparkSession.builder().config(config).getOrCreate()

    val yearDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_patent.csv")
      .distinct()

    val patentDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_cpc_current.csv")

    val filteredDF = yearDF.join(patentDF, Seq("patent_id"))
    filteredDF.createOrReplaceTempView("my_table")

    val combinedDF = spark.sql(
      """
      SELECT YEAR(patent_date) AS year, cpc_group
      FROM my_table
      WHERE YEAR(patent_date) BETWEEN 2002 AND 2005
      """)

    combinedDF.createOrReplaceTempView("combinedDF")
    val cpcCountsDF = spark.sql(
      """
      SELECT year, cpc_group, COUNT(*) AS count
      FROM combinedDF
      GROUP BY year, cpc_group
      """)

    cpcCountsDF.createOrReplaceTempView("cpcCountsDF")

    val sortedDF = spark.sql(
      """
      SELECT year, cpc_group, count
      FROM cpcCountsDF
      ORDER BY year, count DESC
      """)

    val groupedDF = sortedDF.groupBy("year").agg(collect_list(struct("cpc_group", "count")).as("cpc_counts"))

    val top5DF = groupedDF.withColumn("cpc_counts", slice(col("cpc_counts"), 1, 5))
    top5DF.show(20)

    // Convert the resulting DataFrame to JSON and return it as a string
    val jsonString = top5DF.toJSON.collectAsList().toString()
    jsonString
  }
}
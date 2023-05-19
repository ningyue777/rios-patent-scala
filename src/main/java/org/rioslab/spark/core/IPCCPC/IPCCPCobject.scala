package org.rioslab.spark.core.IPCCPC

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IPCCPCobject {
  def run(args: Array[String]): String = {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount SQL Application")
    val spark = SparkSession.builder().config(config).getOrCreate()

    val assigneeDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_assignee_disambiguated.csv")
      .filter(col("disambig_assignee_organization").contains("Arm Limited"))
      .select("patent_id")
      .distinct()

    val patentDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_cpc_current.csv")

    val ipcDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_ipc_at_issue.csv")

    val filteredDF = assigneeDF.join(patentDF, Seq("patent_id"))
      .join(ipcDF, Seq("patent_id"))

    filteredDF.createOrReplaceTempView("my_table")

    val combinedDF = spark.sql(
      """
      SELECT CONCAT(section,ipc_class, subclass, main_group) AS IPC_number, cpc_group
      FROM my_table
      """)

    combinedDF.show()

    combinedDF.createOrReplaceTempView("combinedDF")

    val ipcCountsDF = spark.sql(
      """
      SELECT IPC_number, cpc_group, COUNT(*) AS count
      FROM combinedDF
      GROUP BY IPC_number, cpc_group
      """)

    ipcCountsDF.createOrReplaceTempView("ipcCountsDF")

    val sortedDF = spark.sql(
      """
      SELECT IPC_number, cpc_group, count
      FROM ipcCountsDF
      ORDER BY IPC_number, count DESC
      """)

    val groupedDF = sortedDF.groupBy("IPC_number").agg(collect_list(struct("cpc_group", "count")).as("cpc_counts"))

    val top5DF = groupedDF.withColumn("cpc_counts", slice(col("cpc_counts"), 1, 5))

    top5DF.show(false)

    // Convert the resulting DataFrame to JSON and return it as a string
    val jsonString = top5DF.toJSON.collectAsList().toString()
    jsonString
  }
}

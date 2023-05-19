package org.rioslab.spark.core.IPCCPCrepart

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IPCCPCrepartobject {
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
      SELECT CONCAT(ipc_class, subclass, main_group) AS IPC_number, cpc_group
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
      SELECT IPC_number, cpc_group, count,
      ROW_NUMBER() OVER (PARTITION BY IPC_number ORDER BY count DESC) AS rank
      FROM ipcCountsDF
      """)

    val top5DF = sortedDF.filter("rank <= 5").limit(50)

    top5DF.show()

    // Convert the resulting DataFrame to JSON and return it as a string
    val jsonString = top5DF.toJSON.collectAsList().toString()
    jsonString
  }
}

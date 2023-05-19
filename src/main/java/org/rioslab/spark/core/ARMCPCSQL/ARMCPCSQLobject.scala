package org.rioslab.spark.core.ARMCPCSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ARMCPCSQLobject {
  // This is the main function of the program
  def run(args: Array[String]): String = {
    // Create a Spark configuration
    val config = new SparkConf()
      .setMaster("local[*]") // Set the Spark execution mode to local[*], which automatically determines the number of CPU cores to use
      .setAppName("ARMCPCSQL Application") // Set the application name
    // Create a SparkSession object, which includes the SparkContext for Spark runtime operations
    val spark = SparkSession.builder().config(config).getOrCreate()

    // Create a temporary view for assignee_disambiguated DataFrame
    spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_assignee_disambiguated.csv")
      .createOrReplaceTempView("assignee_disambiguated")

    // Create a temporary view for cpc_current DataFrame
    spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_cpc_current.csv")
      .createOrReplaceTempView("cpc_current")

    // Join assignee_disambiguated and cpc_current views
    val df = spark.sql(
      """
        |SELECT *
        |FROM assignee_disambiguated JOIN cpc_current ON assignee_disambiguated.patent_id = cpc_current.patent_id
        |""".stripMargin)

    df.show(20)

    // Create a temporary view for the filtered DataFrame
    df.createOrReplaceTempView("temp_table")

    // Filter only those patents with assignee === "Arm Limited"
    val filteredDF = spark.sql(
      """
        |SELECT *
        |FROM temp_table
        |WHERE disambig_assignee_organization LIKE '%Arm Limited%'
      """.stripMargin)

    filteredDF.show(20)

    // Create a temporary view for the filtered DataFrame
    filteredDF.createOrReplaceTempView("temp_table")

    // Count the number of rows for each value in the "cpc_group" column
    val rowCounts = spark.sql(
      """
        |SELECT cpc_group, COUNT(*) AS count
        |FROM temp_table
        |GROUP BY cpc_group
      """.stripMargin)

    // Sort the counts in descending order
    val sortedCounts = rowCounts.orderBy(desc("count"))

    sortedCounts.show(70)

    // Convert sorted counts to JSON and collect as a list
    val sortedString = sortedCounts.toJSON.collectAsList().toString()

    sortedString
  }
}

package org.rioslab.spark.core.specifiedassignee

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

package object specifiedassigneeobject {
  def run(args: Array[String]): String = {
    // Create a Spark configuration
    val config = new SparkConf()
      .setMaster("local[*]") // Set the Spark execution mode to local[*], which automatically determines the number of CPU cores to use
      .setAppName("ARMCPCSQL Application") // Set the application name
    // Create a SparkSession object, which includes the SparkContext for Spark runtime operations
    val spark = SparkSession.builder().config(config).getOrCreate()

    val assigneeDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_assignee_disambiguated.csv")
      .filter(col("disambig_assignee_organization").contains("Arm Limited")
        .or(col("disambig_assignee_organization").contains("Intel"))
        .or(col("disambig_assignee_organization").contains("MIPS")))
      .createOrReplaceTempView("assignee_disambiguated")

    val cpcDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .load("/patent/uspto/csv/g_cpc_current.csv")
      .createOrReplaceTempView("cpc_current")

    val df = spark.sql(
      """
        |SELECT *
        |FROM assignee_disambiguated JOIN cpc_current ON assignee_disambiguated.patent_id = cpc_current.patent_id
        |WHERE cpc_group = 'G06F9/30036'
        |""".stripMargin)

    df.createOrReplaceTempView("temp_table")

    val armCount = spark.sql(
      """
        |SELECT COUNT(*) AS count
        |FROM temp_table
        |WHERE disambig_assignee_organization = 'Arm Limited'
        |""".stripMargin)
      .collect()(0)
      .getAs[Long]("count")

    val intelCount = spark.sql(
      """
        |SELECT COUNT(*) AS count
        |FROM temp_table
        |WHERE disambig_assignee_organization = 'Intel'
        |""".stripMargin)
      .collect()(0)
      .getAs[Long]("count")

    val mipsCount = spark.sql(
      """
        |SELECT COUNT(*) AS count
        |FROM temp_table
        |WHERE disambig_assignee_organization = 'MIPS'
        |""".stripMargin)
      .collect()(0)
      .getAs[Long]("count")

    val resultString = s"The number of patents of ARM in this specified field is $armCount.\n" +
      s"The number of patents of Intel in this specified field is $intelCount.\n" +
      s"The number of patents of MIPS in this specified field is $mipsCount."

    resultString
  }
}

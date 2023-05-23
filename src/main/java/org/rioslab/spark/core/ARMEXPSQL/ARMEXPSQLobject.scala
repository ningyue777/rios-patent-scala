package org.rioslab.spark.core.ARMEXPSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ARMEXPSQLobject {
  def run(args: Array[String]): String = {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount SQL Application")
    val spark = SparkSession.builder().config(config).getOrCreate()

    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW assignee_disambiguated
         |USING csv
         |OPTIONS (path '/patent/uspto/csv/g_assignee_disambiguated.csv', header 'true', multiline 'true', escape '"')
         |""".stripMargin)

    spark.sql(
      s"""
         |CREATE OR REPLACE TEMPORARY VIEW patent
         |USING csv
         |OPTIONS (path '/patent/uspto/csv/g_patent.csv', header 'true', multiline 'true', escape '"')
         |""".stripMargin)

    val df = spark.sql(
      """
        |SELECT assignee_disambiguated.*, patent.*,
        |       assignee_disambiguated._c0 AS assignee_disambiguated_c0
        |FROM assignee_disambiguated
        |JOIN patent
        |ON assignee_disambiguated.patent_id = patent.patent_id
        |""".stripMargin)
    df.show()

    val rowdfnumber = spark.sql("SELECT COUNT(*) AS rowdfnumber FROM assignee_disambiguated JOIN patent ON assignee_disambiguated.patent_id = patent.patent_id").collect()(0).getLong(0)
    println(s"The number of rows in the DataFrame is $rowdfnumber.")

    val filtered = spark.sql("SELECT * FROM assignee_disambiguated JOIN patent ON assignee_disambiguated.patent_id = patent.patent_id WHERE assignee_disambiguated.disambig_assignee_organization LIKE '%Arm Limited%'")
    filtered.show(20)

    val filteredDFbytime = spark.sql("SELECT *, __auto_generated_subquery_name.patent_date AS patent_date FROM (SELECT * FROM assignee_disambiguated JOIN patent ON assignee_disambiguated.patent_id = patent.patent_id WHERE assignee_disambiguated.disambig_assignee_organization LIKE '%Arm Limited%') WHERE year(__auto_generated_subquery_name.patent_date) < 2005 ORDER BY __auto_generated_subquery_name.patent_date DESC")
    filteredDFbytime.show(30)


    val sortedstring = filteredDFbytime.toJSON.collectAsList().toString()

    sortedstring
  }
}






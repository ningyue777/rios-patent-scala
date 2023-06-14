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

    val filteredAssigneeDF = spark.sql("SELECT disambig_assignee_organization, patent_id FROM assignee_disambiguated WHERE disambig_assignee_organization LIKE '%Arm Limited%'")
    val filteredPatentDF = spark.sql("SELECT  patent_id, patent_date FROM patent")

    val df = filteredAssigneeDF.join(filteredPatentDF, Seq("patent_id"))

    val rowdfnumber = df.count()


    val filteredDFbytime = df.filter(year(col("patent_date")) < 2005).orderBy(desc("patent_date"))
    val groupedDF = filteredDFbytime.groupBy(year(col("patent_date")).as("year"))
      .agg(collect_list(struct("*")).as("grouped_data"), count("*").as("count"))
      .orderBy(desc("year"))

    val sortedString = filteredDFbytime.toJSON.collectAsList().toString()
    sortedString
  }
}






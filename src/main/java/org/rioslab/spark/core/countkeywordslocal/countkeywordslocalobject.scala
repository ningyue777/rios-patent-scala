package org.rioslab.spark.core.countkeywordslocal

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object countkeywordslocalobject{
  // This is the main function of the program
  def run(args: Array[String]): Unit = {
    // Create a Spark configuration
    val config = new SparkConf()
      .setMaster("local[*]") // Set the Spark execution mode to local[*], which automatically determines the number of CPU cores to use
      .setAppName("ARMCPCSQL Application") // Set the application name
    // Create a SparkSession object, which includes the SparkContext for Spark runtime operations
    val spark = SparkSession.builder().config(config).getOrCreate()

    // Read the CSV file into a DataFrame
    val df = spark.read
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .csv("/Users/ningyuelai/Desktop/cropped/uspto/g_detail_desc_text/g_detail_desc_text_1976.csv")
    //read patentID
    val ID = spark.read
      .option("header", "true")
      .option("multiline", "true")
      .option("escape", "\"")
      .csv("/Users/ningyuelai/Desktop/cropped/uspto/csv")
    val df2 =df.join(ID, Seq("patent_id"))
    df2.show()


    // Create keyword word pairs DataFrame
    val wordPairs = spark.read
      .text("/Users/ningyuelai/Desktop/共同出现两两组合V2（R01） 2/R01A02.txt")
      .select(functions.split(functions.col("value"), ",").as("words"))
      .selectExpr("words[0] as wordA", "words[1] as wordB")
    wordPairs.show(35)


    // Join the DataFrame with wordPairs and calculate word frequency in the description column
    val joinedDf = wordPairs
      .join(df2, functions.expr("description_text LIKE CONCAT('%', wordA, '%') OR description_text LIKE CONCAT('%', wordB, '%')"))
      .groupBy("wordA", "wordB")
      .agg(functions.count("*").as("count"))
      .orderBy(functions.desc("count"))
    joinedDf.show(35)

    val df3= df2
      .join(joinedDf)
      .select("patent_id")
      .distinct()
    df3.show()

    val countjoinedDF = joinedDf.count()
    println(s"The number of rows in the DataFrame is $countjoinedDF.")

    System.in.read()
  }
}

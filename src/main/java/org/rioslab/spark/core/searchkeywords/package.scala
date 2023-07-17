package org.rioslab.spark.core.searchkeywords

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}

object searchkeywordsobject {
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


    // Define the words to search for and their respective weights
    val wordA = "A"
    val weightA = 2.0
    val wordB = "B"
    val weightB = 1.0

    // Filter the rows in df2 that contain wordA and wordB in the description column
    val filteredDf = df.filter(functions.col("description_text").like(s"%${wordA}%") && functions.col("description_text").like(s"%${wordB}%"))
    // Filter the rows in df2 that contain wordA or wordB in the description column
    // val filteredDf = df2.filter(functions.col("description").like(s"%${wordA}%") || functions.col("description").like(s"%${wordB}%"))
    // Calculate the weighted count based on wordA and wordB
    val weightedDf = filteredDf.groupBy("patent_id")
      .agg(
        functions.sum(
          functions.when(functions.col("description_text").like(s"%${wordA}%"), weightA)
            .otherwise(weightB)
        ).as("weightedCount")
      )

    // Order by weightedCount in descending order
    val orderedDf = weightedDf.orderBy(functions.desc("weightedCount"))

    // Select only the patentID column
    val patentIds = orderedDf.select("patent_id")

    // Show the resulting DataFrame
    patentIds.show()

    // Rest of your code...
  }
}
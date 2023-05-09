package org.rioslab.spark.core.wordfrequency

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordfrequencySQL2 {

  // 这里是程序运行的主函数
  def main(args: Array[String]) : Unit = {

    // 创建配置
    val config = new SparkConf() // 创建一个配置类的对象
      .setMaster("local[*]") // 设置spark的运行模式 local[*] 表示本地运行，自动确定使用的CPU核数
      .setAppName("WordCount SQL Application") // 这里设置应用名

    // 这里创建一个Spark Session的对象，里面包含Spark Context，用于Spark运行时的操作
    val spark = SparkSession.builder().config(config).getOrCreate()

    // 这里导入将DataSet转换为DataFrame的一些工具类
    import spark.implicits._

    // 这里创建一个spark的DataFrame
    val df = spark
      .read // 表示读文件
      .option("header", "true") // 设置参数header=true，表示有表头
      .option("multiline", "true") // 设置参数multiline=true，表示一个单元格可能有多行
      // 使用"来转义"
      .option("escape", "\"") // 设置escape="\""，表示使用双引号转义双引号。意思在csv文件里""表示"
      .csv("patent/patent_cleaned.csv") // 读取csv文件

    // filter the rows where cache and coherency appeared
    val wordA = "cache"
    val wordB = "coherency"
    val filteredrows2 = df.filter((col(colName = "abstract").contains(wordA) || col(colName = "description").contains(wordA)) && (col("abstract").contains(wordB) || col("description").contains(wordB)))
    val count2 = filteredrows2.count()
    println(s"The words $wordA and $wordB appeared together in $count2 rows.")






    // filter the rows where either abstract or description column contains both words (RDD)
    val wordC = "cache"
    val wordD = "coherency"

    val filteredRDD = df.rdd.filter(row => {
      val abstractText = row.getString(3)
      val descriptionText = row.getString(4)
      (abstractText.contains(wordC) || descriptionText.contains(wordC)) && (abstractText.contains(wordD) || descriptionText.contains(wordD))
    })

    // count the number of rows
    val count1 = filteredRDD.count()
    println(s"The words $wordC and $wordD appeared together in $count1 rows.")



    System.in.read()



  }
}
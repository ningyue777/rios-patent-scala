package org.rioslab.spark.core.basicwordcount

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountSQLbasic {

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
    val df1 = spark
      .read // 表示读文件
      .option("header", "true") // 设置参数header=true，表示有表头
      .option("multiline", "true") // 设置参数multiline=true，表示一个单元格可能有多行
      // 使用"来转义"
      .option("escape", "\"") // 设置escape="\""，表示使用双引号转义双引号。意思在csv文件里""表示"
      .csv("patent/g_gov_interest.csv") // 读取csv文件
    val df2 = spark
      .read // 表示读文件
      .option("header", "true") // 设置参数header=true，表示有表头
      .option("multiline", "true") // 设置参数multiline=true，表示一个单元格可能有多行
      // 使用"来转义"
      .option("escape", "\"") // 设置escape="\""，表示使用双引号转义双引号。意思在csv文件里""表示"
      .csv("patent/g_gov_interest_org.csv") // 读取csv文件
    val df = df1.join(df2, Seq("patent_id"), "inner")
    df.show(20)
    // 向控制台打印Dataframe



    // count how many rows the DF has
    val numRows = df.count()
    println(s"The number of rows in the DataFrame is $numRows.")



    // 将Dataframe的每一行的第3列（摘要）第4列（描述），（从0开始计数）连接成一个字符串
    val lines = df.map(
      line => line(3).toString + " " + line(4).toString
    )

    val words = lines.flatMap(_.split(" ")) // 根据空格拆分字符串成一个个的单词
    words.show()

    val wordsGroup = words.groupBy("value") // 根据"value"这一个column分组

    val wordCount = wordsGroup
      .count() // 统计单词出现的频率
      .sort(col("count").desc) // 根据count这一个column降序排列
    wordCount.show()


    //移除部分stopwords
    val stopWords = List("a","In", "that","may","or", "by","with","one","are","from","can","which", "an", "the", "in", "on", "at", "to", "of", "for", "and","is","as","be"," ","of"," in","The")

    val wordsFiltered = words
      .filter(!stopWords.contains(_)) // Remove stop words
      .groupBy("value")
      .count()
      .sort(col("count").desc)

    wordsFiltered.show(30)


    System.in.read()



  }
}
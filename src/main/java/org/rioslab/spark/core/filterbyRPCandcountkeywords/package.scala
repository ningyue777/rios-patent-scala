package org.rioslab.spark.core.filterbyRPCandcountkeywords

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object filterbyRPCandcountkeywordsobject {

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
      .csv("/Users/ningyuelai/Desktop/cropped/uspto/csv/g_gov_interest.csv") // 读取csv文件
    val df2 = spark
      .read // 表示读文件
      .option("header", "true") // 设置参数header=true，表示有表头
      .option("multiline", "true") // 设置参数multiline=true，表示一个单元格可能有多行
      // 使用"来转义"
      .option("escape", "\"") // 设置escape="\""，表示使用双引号转义双引号。意思在csv文件里""表示"
      .csv("/Users/ningyuelai/Desktop/cropped/uspto/g_detail_desc_text/g_detail_desc_text_1976.csv") // 读取csv文件
    //val df = df1.join(df2, Seq("patent_id"), "inner")
    //df.show(20)
    // 向控制台打印Dataframe


    //filter DF based on RPC number RDD, 然后看高频词
    val filteredRDDnm = df2.rdd.filter(row => row.getString(1).contains("R01A02"))
    val filteredDFnm = spark.createDataFrame(filteredRDDnm, df2.schema)
    filteredDFnm.show()
    val numRowsfilteredDFnm = filteredDFnm.count()
    println(s"The number of rows in the DataFrame is $numRowsfilteredDFnm.")

    // create Keywords word pairs using files on my laptop, and count the keywords' frequency
    val wordPairs = spark.read.text("/Users/ningyuelai/Desktop/共同出现两两组合V2（R01） 2/R01A02.txt")
      .select(split(col("value"), ",").as("words"))
      .selectExpr("words[0] as wordA", "words[1] as wordB")
    wordPairs.show(30)

    // join the dataframes and add a count column for rows where both words are present
    val joinedDf = wordPairs
      .join(df2, expr("(abstract LIKE CONCAT('%', wordA, '%') OR description LIKE CONCAT('%', wordA, '%')) AND (abstract LIKE CONCAT('%', wordB, '%') OR description LIKE CONCAT('%', wordB, '%'))"))
      .groupBy($"wordA", $"wordB")
      .agg(count("*").as("count"))
    // display the resulting dataframe
    val sortedjoinedDF= joinedDf.orderBy(desc("count"))
    sortedjoinedDF.show(30)
    val countjoinedDF = joinedDf.count()
    println(s"The number of rows in the DataFrame is $countjoinedDF.")

    System.in.read()



  }
}
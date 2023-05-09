package org.rioslab.spark.core.RPCclasscount

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CPCClassCountSQL {
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

    //filter only those patents with assignee===MIPS
    val filteredDF = df.filter(col("assignee").contains("MIPS"))
    filteredDF.show()
    //count the number of rows for each value in the "cpc.code" column
    val rowCounts = filteredDF.groupBy("cpc.code").count()
    rowCounts.show()
    //sort in descending order
    val sortedCounts = rowCounts.orderBy(desc("count"))
    sortedCounts.show(70)
    // Write to a local file
    val outputPath = "/Users/Ningyuelai/Desktop/CPCfiletest1.txt"
    sortedCounts.write.format("csv").option("header", "true").mode("overwrite").save(outputPath)


  }
}

package org.rioslab.spark.core.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCountSQLl {

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
    df.show() // 向控制台打印Dataframe



    // count how many rows the DF has
    val numRows = df.count()
    println(s"The number of rows in the DataFrame is $numRows.")


    // 简单加一栏expiration time做测试
    import org.apache.spark.sql.functions._
    val dfWithExpTime = df.withColumn("expiration time", when(col("rpc_num") === "R01A02", 1).otherwise(2))
    dfWithExpTime.show()
    //将dfwithexptime（测试）的exptime=1数字的栏filter出来·
    val filteredDFwithExp = dfWithExpTime.filter(col("expiration time") === 1)
    filteredDFwithExp.show()

    //将dfwithexptime（测试）的assignee={}数字的栏filter出来·
    //val filteredDFwithassignee = df.filter(col("") === 1)
    //filteredDFwithassignee.show()




    //To count the number of rows for each value in the "rpc_num" column
    val rowCounts = df.groupBy("rpc_num").count()
    rowCounts.show()
    //sort in descending order
    val sortedCounts = rowCounts.orderBy(desc("count"))
    sortedCounts.show()



    // Write to a local file
    val outputPath = "/Users/Ningyuelai/Desktop/filetest.txt"
    rowCounts.write.format("csv").option("header", "true").mode("overwrite").save(outputPath)





    // create Keywords word pairs using files on my laptop, and count the keywords' frequency
    val wordPairs = spark.read.text("/Users/ningyuelai/Desktop/共同出现两两组合V2（R01） 2/R01A01.txt")
      .select(split(col("value"), ",").as("words"))
      .selectExpr("words[0] as wordA", "words[1] as wordB")
    wordPairs.show(30)
    // join the dataframes and add a count column for rows where both words are present
    val joinedDf = wordPairs
      .join(df, expr("(abstract LIKE CONCAT('%', wordA, '%') OR description LIKE CONCAT('%', wordA, '%')) AND (abstract LIKE CONCAT('%', wordB, '%') OR description LIKE CONCAT('%', wordB, '%'))"))
      .groupBy($"wordA", $"wordB")
      .agg(count("*").as("count"))
    // display the resulting dataframe
    joinedDf.show(30)
    val countjoinedDF = joinedDf.count()
    println(s"The number of rows in the DataFrame is $countjoinedDF.")






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
    println(filteredRDD)




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
    val stopWords = List("a", "an", "the", "in", "on", "at", "to", "of", "for", "and","is","as","be")

    val wordsFiltered = words
      .filter(!stopWords.contains(_)) // Remove stop words
      .groupBy("value")
      .count()
      .sort(col("count").desc)

    wordsFiltered.show()



    //filter DF based on RPC number
    val filteredDF = df.filter(col("rpc_num").contains("R01A02"))
    filteredDF.show()



    //filter DF based on RPC number RDD, 然后看高频词
    val filteredRDDnm = df.rdd.filter(row => row.getString(1).contains("R01A02"))
    val filteredDFnm = spark.createDataFrame(filteredRDDnm, df.schema)
    filteredDFnm.show()
    val numRowsfilteredDFnm = filteredDFnm.count()
    println(s"The number of rows in the DataFrame is $numRowsfilteredDFnm.")
    val lines1 = filteredDFnm.map(
      line => line(3).toString + " " + line(4).toString
    )
    val wordsR01 = lines1.flatMap(_.split(" ")) // 根据空格拆分字符串成一个个的单词
    val wordsGroup1 = wordsR01.groupBy("value") // 根据"value"这一个column分组

    val wordCount1 = wordsGroup1
      .count() // 统计单词出现的频率
      .sort(col("count").desc) // 根据count这一个column降序排列
    wordCount1.show(50)



    System.in.read()



  }
}
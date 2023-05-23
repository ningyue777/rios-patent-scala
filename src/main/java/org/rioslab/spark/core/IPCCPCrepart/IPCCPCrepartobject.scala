package org.rioslab.spark.core.IPCCPCrepart

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IPCCPCrepatobject {
  def run(args: Array[String]): String = {
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount SQL Application")
    val spark = SparkSession.builder().config(config).getOrCreate()

    val assigneeRDD = spark.sparkContext.textFile("/patent/uspto/csv/g_assignee_disambiguated.csv")
    val patentRDD = spark.sparkContext.textFile("/patent/uspto/csv/g_cpc_current.csv")
    val ipcRDD = spark.sparkContext.textFile("/patent/uspto/csv/g_ipc_at_issue.csv")

    val assigneeHeader = assigneeRDD.first()
    val patentHeader = patentRDD.first()
    val ipcHeader = ipcRDD.first()

    val assigneeDataRDD = assigneeRDD.filter(_ != assigneeHeader)
    val patentDataRDD = patentRDD.filter(_ != patentHeader)
    val ipcDataRDD = ipcRDD.filter(_ != ipcHeader)

    val assigneePairRDD = assigneeDataRDD.map(line => {
      val fields = line.split(",")
      (fields(0), fields(3))
    }).repartition(10) // Repartition assignee data RDD

    val patentPairRDD = patentDataRDD.map(line => {
      val fields = line.split(",")
      (fields(0), fields(1))
    })

    val ipcPairRDD = ipcDataRDD.map(line => {
      val fields = line.split(",")
      (fields(0), fields(2))
    })

    val joinRDD = assigneePairRDD.join(patentPairRDD).join(ipcPairRDD).repartition(10) // Repartition join RDD

    val combinedRDD = joinRDD.map {
      case (patentId, ((assigneeId, cpcGroup), ipcClass)) =>
        (ipcClass.substring(0, 4), cpcGroup)
    }

    val ipcCountsRDD = combinedRDD.map(pair => (pair, 1)).reduceByKey(_ + _)

    val sortedRDD = ipcCountsRDD.map {
      case ((ipcClass, cpcGroup), count) =>
        (ipcClass, (cpcGroup, count))
    }.sortByKey()

    val groupedRDD = sortedRDD.groupByKey().mapValues(_.toList.sortBy(-_._2).take(5))

    groupedRDD.collect().foreach(println)

    // Convert the resulting RDD to JSON and return it as a string
    val jsonString = groupedRDD.map {
      case (ipcClass, cpcCounts) =>
        val cpcJson = cpcCounts.map {
          case (cpcGroup, count) =>
            s"""{"cpc_group":"$cpcGroup", "count":$count}"""
        }
        s"""{"IPC_number":"$ipcClass", "cpc_counts": [${cpcJson.mkString(",")}]}"""
    }.collect().mkString("[", ",", "]")

    jsonString
  }
}

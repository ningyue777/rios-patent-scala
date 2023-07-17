package org.rioslab.spark.core

import org.rioslab.spark.core.searchkeywords.searchkeywordsobject


object App {

  def main(args: Array[String]): Unit = {

    val res = searchkeywordsobject.run(args)

 println(res)
  }
}
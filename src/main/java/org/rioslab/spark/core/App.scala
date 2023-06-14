package org.rioslab.spark.core

import org.rioslab.spark.core.specifiedassignee.specifiedassigneeobject


object App {

  def main(args: Array[String]): Unit = {

    val res = specifiedassigneeobject.run(args)

 println(res)
  }
}
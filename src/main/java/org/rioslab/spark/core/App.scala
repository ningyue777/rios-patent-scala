package org.rioslab.spark.core

import org.rioslab.spark.core.ARMCPCSQL.ARMCPCSQLobject


object App {

  def main(args: Array[String]): Unit = {

    val res = ARMCPCSQLobject.run(args)

 println(res)
  }


}
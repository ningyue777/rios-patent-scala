package org.rioslab.spark.core

import org.rioslab.spark.core.MIPSCPC.MIPSCPCobject


object App {

  def main(args: Array[String]): Unit = {

    val res = MIPSCPCobject.run(args)

 println(res)
  }


}
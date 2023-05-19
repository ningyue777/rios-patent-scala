package org.rioslab.spark.core

import org.rioslab.spark.core.IPCCPC.IPCCPCobject


object App {

  def main(args: Array[String]): Unit = {

    val res = IPCCPCobject.run(args)

 println(res)
  }


}
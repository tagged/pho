package com.tagged.pho

import org.apache.hadoop.hbase.util.Bytes

case class Qualifier(name: String) {

  lazy val bytes = Bytes.toBytes(name)

}

object Qualifier {

  def apply(bytes: Array[Byte]): Qualifier = {
    val name = Bytes.toString(bytes)
    Qualifier(name)
  }

}

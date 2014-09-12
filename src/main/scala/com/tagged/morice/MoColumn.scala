package com.tagged.morice

import org.apache.hadoop.hbase.client.Result

case class MoColumn[T](family: MoColumnFamily, name: String, converter: MoConverter[T]) {

  lazy val getBytes: Array[Byte] = name.getBytes

  def getCell(result: Result): MoCell[T] = {
    val bytes = result.getValue(family.getBytes, getBytes)
    val value = converter.getValue(bytes)
    new MoCell(this, value)
  }

}

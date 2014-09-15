package com.tagged.morice

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

case class MoColumn[T](family: MoColumnFamily, name: String, converter: MoConverter[T]) {

  lazy val toBytes: Array[Byte] = Bytes.toBytes(name)

  def getCell(result: Result): MoCell[T] = {
    val bytes = result.getValue(family.toBytes, toBytes)
    val value = converter.getValue(bytes)
    new MoCell(this, value)
  }

}

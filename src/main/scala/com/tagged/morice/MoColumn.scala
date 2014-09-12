package com.tagged.morice

import org.apache.hadoop.hbase.client.Result

case class MoColumn[T](family: MoColumnFamily, name: String, converter: MoConverter[T]) {

  lazy val getBytes: Array[Byte] = name.getBytes

  def getCell(result: Result): MoCell[T] = getCell(result.getValue(family.getBytes, getBytes))

  def getCell(bytes: Array[Byte]): MoCell[T] = new MoCell(this, converter.getValue(bytes))

}

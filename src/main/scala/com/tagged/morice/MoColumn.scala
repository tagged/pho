package com.tagged.morice

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

case class MoColumn[T](family: MoColumnFamily, name: String, converter: MoConverter[T]) {

  lazy val toBytes: Array[Byte] = Bytes.toBytes(name)

  def getCell(result: Result): Option[MoCell[T]] = {
    result.getValue(family.toBytes, toBytes) match {
      case null => None
      case bytes =>
        converter.getValue(bytes) match {
          case null => None
          case value => Some(MoCell(this, value))
        }
    }
  }

}

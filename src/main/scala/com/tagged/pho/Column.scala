package com.tagged.pho

import com.tagged.pho.converter.PhoConverter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

case class Column[T](family: ColumnFamily, name: String, converter: PhoConverter[T]) {

  lazy val toBytes: Array[Byte] = Bytes.toBytes(name)

  def getCell(result: Result): Option[Cell[T]] = {
    result.getValue(family.toBytes, toBytes) match {
      case null => None
      case bytes =>
        converter.getValue(bytes) match {
          case null => None
          case value => Some(Cell(this, value))
        }
    }
  }

}

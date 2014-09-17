package com.tagged.pho

import com.tagged.pho.converter.PhoConverter
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

/**
 * A column defines its identifier (family, qualifier)
 * and its conversion between high-level value and storage-level byte array.
 *
 * @param family    column group
 * @param qualifier column name
 * @param converter for translating from bytes to values and vice-versa
 * @tparam A        the high-level type stored in this column
 */
case class Column[A](family: ColumnFamily, qualifier: String, converter: PhoConverter[A]) {

  lazy val qualifierBytes: Array[Byte] = Bytes.toBytes(qualifier)

  def getCell(result: Result): Option[Cell[A]] = {
    result.getValue(family.bytes, qualifierBytes) match {
      case null => None
      case bytes =>
        converter.getValue(bytes) match {
          case null => None
          case value => Some(Cell(this, value))
        }
    }
  }

}

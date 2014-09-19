package com.tagged.pho

import com.tagged.pho.converter.PhoConverter
import org.apache.hadoop.hbase.client.Result

case class RowKey[T](converter: PhoConverter[T], value: T) {

  def toBytes: Array[Byte] = converter.toBytes(value)

  def getRowKey(result: Result): RowKey[T] = {
    val bytes = result.getRow
    val value = converter.getValue(bytes)
    copy(value = value)
  }

}

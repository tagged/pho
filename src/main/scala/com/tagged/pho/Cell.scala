package com.tagged.pho

import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter, Filter}

case class Cell[T](column: Column[T], value: T) {

  def toBytes: Array[Byte] = column.converter.toBytes(value)

  def equalsFilter: Filter = new SingleColumnValueFilter(
    column.family.bytes,
    column.qualifierBytes,
    CompareFilter.CompareOp.EQUAL,
    toBytes
  )

}

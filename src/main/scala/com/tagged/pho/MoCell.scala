package com.tagged.pho

import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter, Filter}

case class MoCell[T](column: MoColumn[T], value: T) {

  def toBytes: Array[Byte] = column.converter.toBytes(value)

  def equalsFilter: Filter = new SingleColumnValueFilter(
    column.family.toBytes,
    column.toBytes,
    CompareFilter.CompareOp.EQUAL,
    toBytes
  )

}

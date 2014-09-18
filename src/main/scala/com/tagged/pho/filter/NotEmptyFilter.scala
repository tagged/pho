package com.tagged.pho.filter

import com.tagged.pho.Column
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{SingleColumnValueFilter, Filter}

case class NotEmptyFilter(column: Column[_]) extends PhoFilter {

  override def getFilter: Filter = {
    val filter = new SingleColumnValueFilter(column.family.bytes, column.qualifierBytes, CompareOp.NOT_EQUAL, Array[Byte]())
    filter.setFilterIfMissing(true)
    filter
  }

}

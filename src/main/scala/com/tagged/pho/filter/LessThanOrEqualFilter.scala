package com.tagged.pho.filter

import com.tagged.pho.Cell
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter, Filter}

case class LessThanOrEqualFilter(cell: Cell[_]) extends PhoFilter {

  override def getFilter: Filter = {
    val filter = new SingleColumnValueFilter(
      cell.column.family.bytes,
      cell.column.qualifierBytes,
      CompareFilter.CompareOp.LESS_OR_EQUAL,
      cell.valueBytes
    )
    filter.setFilterIfMissing(true)
    filter
  }

}

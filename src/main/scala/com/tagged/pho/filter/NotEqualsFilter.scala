package com.tagged.pho.filter

import com.tagged.pho.Cell
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}

case class NotEqualsFilter(cell: Cell[_]) extends PhoFilter {

  lazy val getFilter = new SingleColumnValueFilter(
    cell.column.family.bytes,
    cell.column.qualifierBytes,
    CompareFilter.CompareOp.NOT_EQUAL,
    cell.valueBytes
  )

}

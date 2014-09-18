package com.tagged.pho.filter

import com.tagged.pho.Cell
import org.apache.hadoop.hbase.filter.{Filter, CompareFilter, SingleColumnValueFilter}

/**
 * If the column value does not exist,
 * the row will be filtered
 * as a non-existent column value cannot be said to equal something.
 * This is different from standard SQL logic.
 */
case class EqualsFilter(cell: Cell[_]) extends PhoFilter {

  def getFilter: Filter = {
    val filter = new SingleColumnValueFilter(
      cell.column.family.bytes,
      cell.column.qualifierBytes,
      CompareFilter.CompareOp.EQUAL,
      cell.valueBytes
    )
    filter.setFilterIfMissing(true)
    filter
  }

}

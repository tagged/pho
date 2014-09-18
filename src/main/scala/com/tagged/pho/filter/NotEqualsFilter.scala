package com.tagged.pho.filter

import com.tagged.pho.Cell
import org.apache.hadoop.hbase.filter.{Filter, CompareFilter, SingleColumnValueFilter}

/**
 * If the column value does not exist,
 * the row will NOT be filtered
 * as a non-existent row could be said to not equal the given value.
 */
case class NotEqualsFilter(cell: Cell[_]) extends PhoFilter {

  def getFilter: Filter = {
    val filter = new SingleColumnValueFilter(
      cell.column.family.bytes,
      cell.column.qualifierBytes,
      CompareFilter.CompareOp.NOT_EQUAL,
      cell.valueBytes
    )
    filter.setFilterIfMissing(false)
    filter
  }

}

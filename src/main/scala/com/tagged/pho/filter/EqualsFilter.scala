package com.tagged.pho.filter

import com.tagged.pho.Cell
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter}

case class EqualsFilter(cell: Cell[_]) extends PhoFilter {

  /**
   * TODO: It may be preferable to move this to PhoConverter,
   *       to support complex equality operations.
   */
  lazy val getFilter = new SingleColumnValueFilter(
    cell.column.family.bytes,
    cell.column.qualifierBytes,
    CompareFilter.CompareOp.EQUAL,
    cell.valueBytes
  )

}

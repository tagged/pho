package com.tagged.pho

import com.tagged.pho.filter.{NoOpFilter, PhoFilter}
import org.apache.hadoop.hbase.client.Scan

case class Query[A](
                     startRow: RowKey[A],
                     endRow: RowKey[_],
                     columns: Seq[Column[_]],
                     filter: PhoFilter = NoOpFilter
                     ) {

  def getScan = {
    val scan = new Scan(startRow.toBytes, endRow.toBytes)
    for (column <- columns) {
      scan.addColumn(column.family.bytes, column.qualifierBytes)
    }
    filter.addFilterTo(scan)
    scan
  }

}

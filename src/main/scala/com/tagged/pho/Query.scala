package com.tagged.pho

import com.tagged.pho.filter.PhoFilter
import org.apache.hadoop.hbase.client.Scan

case class Query[A](
                     startRow: RowKey[A],
                     endRow: RowKey[_],
                     columns: Seq[Column[_]],
                     filters: Seq[PhoFilter] = Seq()
                     ) {

  def getScan = {
    val scan = new Scan(startRow.toBytes, endRow.toBytes)
    for (column <- columns) {
      scan.addColumn(column.family.bytes, column.qualifierBytes)
    }
    for (filter <- filters) {
      filter.addFilterTo(scan)
    }
    scan
  }

}

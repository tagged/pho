package com.tagged.pho

import com.tagged.pho.filter.PhoFilter
import org.apache.hadoop.hbase.client.Scan

case class Query[A](
                         startRow: RowKey[A],
                         endRow: RowKey[_],
                         columns: Iterable[Column[_]],
                         filters: Iterable[PhoFilter] = Seq()
                         ) {

  lazy val getScan = {
    val scan = new Scan(startRow.toBytes, endRow.toBytes)
    for (column <- columns) {
      scan.addColumn(column.family.toBytes, column.toBytes)
    }
    for (filter <- filters) {
      filter.addFilterTo(scan)
    }
    scan
  }

}

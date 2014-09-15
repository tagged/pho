package com.tagged.morice

import org.apache.hadoop.hbase.client.Scan

case class MoQuery[A,B](
                         startRow: MoRowKey[A],
                         endRow: MoRowKey[A],
                         columns: Iterable[MoColumn[B]],
                         filters: Iterable[MoFilter] = Seq()
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

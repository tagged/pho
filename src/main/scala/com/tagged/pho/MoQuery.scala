package com.tagged.pho

import org.apache.hadoop.hbase.client.Scan

case class MoQuery[A](
                         startRow: MoRowKey[A],
                         endRow: MoRowKey[_],
                         columns: Iterable[MoColumn[_]],
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

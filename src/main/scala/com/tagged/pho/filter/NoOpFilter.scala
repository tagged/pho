package com.tagged.pho.filter

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.Filter

case object NoOpFilter extends PhoFilter {

  override def getFilter: Filter = ???

  override def addFilterTo(scan: Scan) = Unit

}

package com.tagged.pho.filter

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{FilterList, Filter}

trait PhoFilter {

  def getFilter: Filter

  def addFilterTo(scan: Scan): Unit = {
    val addendum = getFilter
    scan.getFilter match {
      case current: Filter => scan.setFilter(new FilterList(current, addendum))
      case null => scan.setFilter(addendum)
    }
  }

}

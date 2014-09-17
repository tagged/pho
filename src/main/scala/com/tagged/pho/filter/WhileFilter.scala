package com.tagged.pho.filter

import org.apache.hadoop.hbase.filter.{WhileMatchFilter, Filter}

case class WhileFilter(x: PhoFilter) extends PhoFilter {

  def getFilter: Filter = new WhileMatchFilter(x.getFilter)

}

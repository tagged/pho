package com.tagged.pho.filter

import org.apache.hadoop.hbase.filter.{FilterList, Filter}

case class OrFilter(filters: PhoFilter*) extends PhoFilter {

  def getFilter: Filter = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters.map(_.getFilter): _*)

  override def or(next: PhoFilter) = OrFilter(filters :+ next: _*)

}

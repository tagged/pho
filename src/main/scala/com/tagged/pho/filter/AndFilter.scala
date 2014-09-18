package com.tagged.pho.filter

import org.apache.hadoop.hbase.filter.{FilterList, Filter}

case class AndFilter(filters: PhoFilter*) extends PhoFilter {

  def getFilter: Filter = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters.map(_.getFilter): _*)

  override def and(next: PhoFilter) = AndFilter(filters :+ next: _*)

}

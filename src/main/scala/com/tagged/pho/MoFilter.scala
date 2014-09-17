package com.tagged.pho

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter._

trait MoFilter {

  def getFilter: Filter

  def addFilterTo(scan: Scan) = {
    val addendum = getFilter
    scan.getFilter match {
      case list: FilterList => list.addFilter(addendum)
      case current: Filter => scan.setFilter(new FilterList(current, addendum))
      case null => scan.setFilter(addendum)
    }
  }

}

object MoFilter {

  case class CellEqualsFilter(cell: MoCell[_]) extends MoFilter {

    lazy val getFilter = cell.equalsFilter

  }

  case class LimitFilter(limit: Int) extends MoFilter {

    // TODO: Note that this filter cannot guarantee that the number of results returned to a client are <= page size.
    //       This is because the filter is applied separately on different region servers.
    //       It does however optimize the scan of individual HRegions by making sure that the page size is never exceeded locally.
    //       https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/PageFilter.html
    lazy val getFilter = new PageFilter(limit)

  }

  case class OrFilter(filters: MoFilter*) extends MoFilter {

    lazy val getFilter: Filter = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters.map(_.getFilter): _*)

  }

}

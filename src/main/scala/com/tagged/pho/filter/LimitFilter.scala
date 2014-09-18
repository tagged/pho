package com.tagged.pho.filter

import org.apache.hadoop.hbase.filter.{Filter, PageFilter}

case class LimitFilter(limit: Int) extends PhoFilter {

  // TODO: Note that this filter cannot guarantee that the number of results returned to a client are <= page size.
  //       This is because the filter is applied separately on different region servers.
  //       It does however optimize the scan of individual HRegions by making sure that the page size is never exceeded locally.
  //       https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/PageFilter.html
  def getFilter: Filter = new PageFilter(limit)

}

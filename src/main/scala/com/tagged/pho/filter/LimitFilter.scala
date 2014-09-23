/*
 * Copyright 2014 Tagged
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tagged.pho.filter

import org.apache.hadoop.hbase.filter.{Filter, PageFilter}

case class LimitFilter(limit: Int) extends PhoFilter {

  // TODO: Note that this filter cannot guarantee that the number of results returned to a client are <= page size.
  //       This is because the filter is applied separately on different region servers.
  //       It does however optimize the scan of individual HRegions by making sure that the page size is never exceeded locally.
  //       https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/filter/PageFilter.html
  def getFilter: Filter = new PageFilter(limit)

}

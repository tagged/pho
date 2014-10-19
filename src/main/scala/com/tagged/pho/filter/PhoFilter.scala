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

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.{Filter, FilterList}

trait PhoFilter {

  def getFilter: Filter

  def addFilterTo(scan: Scan): Unit = {
    val addendum = getFilter
    scan.getFilter match {
      case current: Filter => scan.setFilter(new FilterList(current, addendum))
      case null => scan.setFilter(addendum)
    }
  }

  def and(next: PhoFilter) = AndFilter(this, next)

  def or(next: PhoFilter) = OrFilter(this, next)

}

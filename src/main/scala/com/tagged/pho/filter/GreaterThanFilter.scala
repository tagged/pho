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

import com.tagged.pho.Cell
import org.apache.hadoop.hbase.filter.{CompareFilter, SingleColumnValueFilter, Filter}
import scala.language.existentials

case class GreaterThanFilter(cell: Cell[_]) extends PhoFilter {

  override def getFilter: Filter = {
    val filter = new SingleColumnValueFilter(
      cell.column.family.bytes,
      cell.column.qualifierBytes,
      CompareFilter.CompareOp.GREATER,
      cell.valueBytes
    )
    filter.setFilterIfMissing(true)
    filter
  }

}

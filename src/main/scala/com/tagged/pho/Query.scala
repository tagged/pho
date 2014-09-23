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

package com.tagged.pho

import com.tagged.pho.filter.{NoOpFilter, PhoFilter}
import org.apache.hadoop.hbase.client.Scan

case class Query[A](
                     startRow: RowKey[A],
                     endRow: RowKey[_],
                     columns: Seq[Column[_]],
                     filter: PhoFilter = NoOpFilter
                     ) {

  def getScan = {
    val scan = new Scan(startRow.toBytes, endRow.toBytes)
    for (column <- columns) {
      scan.addColumn(column.family.bytes, column.qualifierBytes)
    }
    filter.addFilterTo(scan)
    scan
  }

}

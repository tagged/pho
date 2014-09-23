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

import org.apache.hadoop.hbase.client.Put

/**
 * Grouped by distinct row key,
 * a document represents a set of cell values stored inside of a row.
 *
 * @param key
 * @param cells
 * @param version
 * @tparam A
 */
case class Document[A](key: RowKey[A], cells: Seq[Cell[_]], version: Option[Version] = None) {

  def this(key: RowKey[A], cells: Seq[Cell[_]], version: Version) = this(key, cells, Option(version))

  /**
   * Retrieve a specific column value from this document.
   *
   * @param column identifies a cell from which to retrieve a value
   * @tparam X     the value type being retrieved
   * @return       the optional value if it exists
   */
  def getValue[X](column: Column[X]): Option[X] = {
    cells.find(_.column == column) match {
      case Some(cell) => Option(cell.value.asInstanceOf[X])
      case None => None
    }
  }

  /**
   * Generate a Put object from this keyed collection of cells.
   *
   * @return HBase Put
   */
  def getPut: Put = {
    val put = version match {
      case Some(v) => new Put(key.toBytes, v.timestamp)
      case None    => new Put(key.toBytes)
    }
    for (value <- cells) {
      value.addToPut(put)
    }
    put
  }

}

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

import com.tagged.pho.converter.PhoConverter
import org.apache.hadoop.hbase.client.Result

/**
 * A column defines its identifier (family, qualifier)
 * and its conversion between high-level value and storage-level byte array.
 *
 * @param family    column group
 * @param qualifier column name
 * @param converter for translating from bytes to values and vice-versa
 * @tparam A        the high-level type stored in this column
 */
case class Column[A](family: ColumnFamily, qualifier: Qualifier, converter: PhoConverter[A]) {

  def getCell(result: Result): Option[Cell[A]] = {
    result.getValue(family.bytes, qualifier.bytes) match {
      case null => None
      case bytes =>
        converter.getValue(bytes) match {
          case null => None
          case value => Some(Cell(this, value))
        }
    }
  }

  def getCell(bytes: Array[Byte]): Cell[A] = {
    val value = converter.getValue(bytes)
    Cell(this, value)
  }

}

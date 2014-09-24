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
 * Identified by column location, value and version timestamp,
 * a cell represents the basic unit of storage.
 *
 * @param column location of data
 * @param value  high-level piece of data
 * @tparam A     type of data
 */
case class Cell[A](column: Column[A], value: A, version: Option[Version] = None) {

  def this(column: Column[A], value: A, version: Version) = this(column, value, Option(version))

  def valueBytes: Array[Byte] = column.converter.toBytes(value)

  def addToPut(put: Put) = version match {
    case Some(v) => put.add(column.family.bytes, column.qualifier.bytes, v.timestamp, valueBytes)
    case None    => put.add(column.family.bytes, column.qualifier.bytes, valueBytes)
  }

}

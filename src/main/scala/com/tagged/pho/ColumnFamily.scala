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

import org.apache.hadoop.hbase.util.Bytes

/**
 * A grouping of columns,
 * contained within a shared unit of physical storage.
 *
 * @param name of the family
 */
case class ColumnFamily(name: String) {

  lazy val bytes = Bytes.toBytes(name)

}

object ColumnFamily {

  def apply(bytes: Array[Byte]): ColumnFamily = {
    val name = Bytes.toString(bytes)
    ColumnFamily(name)
  }

}

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

import com.tagged.pho.converter.{PhoConverter, IdentityConverter}
import org.apache.hadoop.hbase.client.Result
import scala.collection.JavaConverters._

class ResultReader[A](rowKeyConverter: PhoConverter[A], columns: Seq[Column[_]]) {

  val columnMap: Map[ColumnFamily,Map[Qualifier,Column[_]]] = columns
    .groupBy(_.family)
    .mapValues(_.map({ column =>
    column.qualifier -> column
  }).toMap)
  
  def getColumn(family: ColumnFamily, qualifier: Qualifier): Column[_] = {
    columnMap.get(family) match {
      case Some(columns) => columns.get(qualifier) match {
        case Some(column) => column
        case None => Column(family, qualifier, IdentityConverter)
      }
      case None => Column(family, qualifier, IdentityConverter)
    }
  }

  def apply(result: Result): Document[A] = {
    val key = RowKey(rowKeyConverter, result.getRow)
    val cells = for ((familyBytes: Array[Byte], qualifiers) <- result.getMap.asScala) yield {
      val family = ColumnFamily(familyBytes)
      for ((qualifierBytes: Array[Byte], values) <- qualifiers.asScala) yield {
        val qualifier = Qualifier(qualifierBytes)
        val column = getColumn(family, qualifier)
        for ((version: java.lang.Long, valueBytes: Array[Byte]) <- values.asScala) yield {
          column.getCell(valueBytes)
        }
      }
    }
    Document(key, cells.flatten.flatten.toSeq)
  }

}

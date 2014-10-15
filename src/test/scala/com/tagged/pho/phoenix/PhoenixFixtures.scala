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

package com.tagged.pho.phoenix

import com.tagged.pho.TestFixtures
import java.sql.{ResultSet, Connection, DriverManager}

/**
 * To make it possible to get a phoenix jdbc connection,
 * the PhoenixDriver must be available on the classpath.
 */
object PhoenixFixtures {

  val phoenixIsAvailable = try {
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
    true  // PhoenixDriver is available
  } catch {
    case _: ClassNotFoundException =>
      false  // PhoenixDriver is missing
    case t: Throwable =>
      t.printStackTrace()  // something else went wrong
      false
  }

  val dsn = "jdbc:phoenix:" + TestFixtures.hbaseZookeeperQuorum + ":/hbase"

  def withConnection[A](f: Connection => A): A = {
    val connection = DriverManager.getConnection(dsn)
    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  /* Generator for mapping ResultSet rows */
  implicit class ResultIterator(set: ResultSet) extends Iterator[ResultSet] {
    def hasNext: Boolean = set.next()
    def next(): ResultSet = set
  }

  def select(sql: String): List[Map[String,Any]] = {
    withConnection({ connection =>
      val statement = connection.prepareStatement(sql)
      val result = statement.executeQuery()
      val meta = result.getMetaData()
      val columnCount = meta.getColumnCount()
      val columns = for (i <- 1 to columnCount) yield {
        meta.getColumnName(i)
      }
      result.map({ row =>
        columns.map({ column =>
          (column, row.getObject(column))
        }).toMap
      }).toList
    })
  }

  def execute(sql: String): Unit = {
    withConnection({ connection =>
      val statement = connection.prepareStatement(sql)
      statement.execute()
    })
  }

}

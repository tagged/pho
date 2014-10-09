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

import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HConnectionManager}

/**
 * These tests require that a test table be pre-created on the HBase cluster.
 * Run the following in the hbase shell.
 *
 *     create 'PhoIntegrationTests', 'family1', 'family2'
 */
object TestFixtures {

  val testTableName = "PhoIntegrationTests"
  val family1 = ColumnFamily("family1")
  val family2 = ColumnFamily("family2")

  val configuration = HBaseConfiguration.create()
  scala.util.Properties.propOrNone("hbase.zookeeper.quorum") match {
    case Some(quorum) => configuration.set("hbase.zookeeper.quorum", quorum)
    case None => Unit
  }

  val admin = new HBaseAdmin(configuration)
  if (!admin.tableExists(testTableName)) {
    val descriptor = new HTableDescriptor(TableName.valueOf(testTableName))
    descriptor.addFamily(new HColumnDescriptor(family1.bytes))
    descriptor.addFamily(new HColumnDescriptor(family2.bytes))
    admin.createTable(descriptor)
  }

  val connection = HConnectionManager.createConnection(configuration)
  val testTable = new PhoTable(connection, testTableName)

}

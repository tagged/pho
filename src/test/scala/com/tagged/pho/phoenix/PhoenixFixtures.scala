package com.tagged.pho.phoenix

import com.tagged.pho.TestFixtures
import java.sql.{Connection, DriverManager}

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

}

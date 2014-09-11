name := "morice"

version := "0.1.0"

libraryDependencies ++= {
  val hadoopV = "2.4.1"
  val hbaseV = "0.98.5-hadoop2"
  Seq(
    "org.apache.hadoop" % "hadoop-common" % hadoopV,
    "org.apache.hbase" % "hbase-common" % hbaseV,
    "org.apache.hbase" % "hbase-client" % hbaseV,
    "org.specs2" %% "specs2" % "2.4.2" % "test"
  )
}

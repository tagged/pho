name := "pho"

organization := "com.tagged"

version := "0.2.7"

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

crossPaths := false

publishMavenStyle := true

publishTo := Some("artifactory.tagged.com" at "https://artifactory.tagged.com/artifactory/ext-release-local")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

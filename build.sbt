name := "pho"

organization := "com.tagged"

version := "0.2.8"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies ++= {
  val hadoopV = "2.5.1"
  val hbaseV = "0.98.6.1-hadoop2"
  Seq(
    "org.apache.hadoop" % "hadoop-common" % hadoopV,
    "org.apache.hbase" % "hbase-common" % hbaseV,
    "org.apache.hbase" % "hbase-client" % hbaseV,
    "org.apache.hbase" % "hbase-server" % hbaseV, // required by phoenix 4.0 client
    "org.specs2" %% "specs2" % "2.4.2" % "test"
  )
}

unmanagedClasspath in Test ++= {
  // let developers append to the classpath for phoenix (and other) testing
  System.getenv("TEST_CLASSPATH") match {
    case null => Seq()
    case cp =>
      val addendum = cp.split(System.getProperty("path.separator")).map(file).toSeq
      streams.value.log.info("unmanagedClasspath in Test ++= " + addendum.mkString(", "))
      addendum
  }
}

crossPaths := false

publishMavenStyle := true

publishTo := Some("artifactory.tagged.com" at "https://artifactory.tagged.com/artifactory/ext-release-local")

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

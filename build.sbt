name := "spark-hbase-examples"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies += "com.typesafe.scala-logging" % "scala-logging-slf4j_2.11" % "2.1.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.5.2"

libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"

libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.2"

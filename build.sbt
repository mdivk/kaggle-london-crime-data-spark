import Dependencies._

organization := "org.sharpsw.spark"

name := "kaggle-london-crime-data-spark"

val appVersion = "1.1.3.0"

val appName = "kaggle-london-crime-data-spark"

version := appVersion

scalaVersion := "2.11.8"

resolvers += Classpaths.typesafeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.2.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.1",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.354",
  
  "com.microsoft.azure" % "azure-storage" % "6.1.0",
  //"org.apache.hadoop" % "hadoop-aws" % "2.7.5",
  //"com.amazonaws" % "aws-java-sdk" % "1.7.4",
  scalaTest % Test
)

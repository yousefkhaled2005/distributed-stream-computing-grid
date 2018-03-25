name := "distributed-stream-computing-grid"

version := "3.2.0"

scalaVersion := "2.12.18"

organization := "org.enterprise.stream"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.16" % Test
)

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint"
)

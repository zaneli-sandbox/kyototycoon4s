organization := "com.zaneli"

name := "kyototycoon4s"

scalaVersion := "2.11.6"

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.8",
  "-feature",
  "-deprecation",
  "-unchecked",
  "-Xlint")

javacOptions ++= Seq(
  "-encoding", "utf-8",
  "-source", "1.8",
  "-target", "1.8"
)

libraryDependencies ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "2.0.0",
  "org.scalaj" %% "scalaj-http" % "1.1.4"
)

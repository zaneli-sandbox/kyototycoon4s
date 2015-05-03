organization := "com.zaneli"

name := "kyototycoon4s"

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-encoding", "UTF-8", "-feature", "-deprecation", "-unchecked")

libraryDependencies ++= Seq(
  "org.scalaj" %% "scalaj-http" % "1.1.4"
)

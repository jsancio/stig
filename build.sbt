name := "stig"

scalaVersion := "2.10.3"

assemblySettings

libraryDependencies ++= Seq(
  "org.clapper" %% "grizzled-slf4j" % "1.0.1",
  "com.amazonaws" % "aws-java-sdk" % "1.7.0",
  "org.apache.httpcomponents" % "httpclient" % "4.3.2" % "runtime",
  "ch.qos.logback" % "logback-classic" % "1.1.1",
  "org.slf4j" % "jcl-over-slf4j" % "1.7.6",
  "org.joda" % "joda-convert" % "1.6",
  "joda-time" % "joda-time" % "2.3")


scalariformSettings

scalacOptions ++= Seq("-deprecation")

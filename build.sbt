name := "workflow"

scalaVersion := "2.10.3"

scalariformSettings

assemblySettings

libraryDependencies ++= Seq(
  "org.clapper" %% "grizzled-slf4j" % "1.0.1",
  "com.amazonaws" % "aws-java-sdk" % "1.7.0",
  "org.joda" % "joda-convert" % "1.6",
  "joda-time" % "joda-time" % "2.3")

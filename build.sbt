name := "JsonExplorer"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  // Googles Json parser
  "com.google.code.gson" % "gson" % "2.8.5",

  // NMF
  "com.github.haifengl" %% "smile-scala" % "1.5.1",
  "org.scalanlp" %% "breeze" % "0.13.2",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),

  // Spark
  "org.apache.spark" %% "spark-core" % "2.3.2"
)
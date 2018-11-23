name := "JsonExplorer"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.google.code.gson" % "gson" % "2.8.5",
  //"org.scalafx" %% "scalafx" % "8.0.144-R12",

  // NMF
  //"com.github.haifengl" %% "smile-scala" % "1.5.1",
  //"org.scalanlp" %% "breeze" % "0.13.2",
  //"com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),

  // Spark
  "org.apache.spark" %% "spark-core" % "2.3.2"
)
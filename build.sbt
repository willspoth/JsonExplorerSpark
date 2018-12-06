name := "JsonExplorer"

version := "0.1"

scalaVersion := "2.11.12"


Compile/mainClass := Some("SparkMain")


libraryDependencies ++= Seq(
  // Googles Json parser
  "com.google.code.gson" % "gson" % "2.8.5",

  // NMF
  "com.github.haifengl" %% "smile-scala" % "1.5.1",
  "org.scalanlp" %% "breeze" % "0.13.2",
  "com.github.fommil.netlib" % "all" % "1.1.2",

  // Spark
  "org.apache.spark" %% "spark-core" % "2.3.2"
)

test in assembly := {}
assemblyJarName in assembly := "JsonExplorer.jar"
mainClass in assembly := Some("SparkMain")
val nettyMeta = ".*META-INF\\/io\\.netty.*".r
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("io", "netty", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case PathList("ch", "qos", xs @ _*) => MergeStrategy.first
  case PathList("org", "slf4j", xs @ _*) => MergeStrategy.first
  case PathList("org", "codehaus", xs @ _*) => MergeStrategy.last
  case PathList("com", "googlecode", xs @ _*) => MergeStrategy.last
  case "overview.html" => MergeStrategy.rename
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case nettyMeta() => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

// java -cp libs\scala-library-2.11.12.jar;libs\spark-core_2.11-2.3.2.jar;libs\hadoop-common-2.7.7.jar;target\scala-2.11\JsonExplorer.jar SparkMain C:\Users\Will\Documents\GitHub\JsonExplorer\clean/yelp10000.merged
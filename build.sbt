name := "JsonExplorer"

version := "0.1"

scalaVersion := "2.11.12"

Compile/mainClass := Some("JsonExplorer.SparkMain")


libraryDependencies ++= Seq(
  // NMF
  "com.github.haifengl" %% "smile-scala" % "1.5.1",
  "org.scalanlp" %% "breeze" % "0.13.2",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly(),

  // Spark
  "org.apache.spark" %% "spark-core" % "2.3.2"
)

test in assembly := {}
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheUnzip = false)
assemblyJarName in assembly := "JsonExplorer.jar"
mainClass in assembly := Some("JsonExplorer.SparkMain")
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
  case PathList("org", "xmlpull", xs @ _*)           => MergeStrategy.discard
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

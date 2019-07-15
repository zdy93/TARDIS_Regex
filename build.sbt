import sbtassembly.AssemblyKeys.assemblyJarName

name := "TARDIS"

version := "1.0"

//scalaVersion := "2.12.2"
scalaVersion := "2.11.8"

scalacOptions += "-target:jvm-1.7"

libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.11"  % "2.1.1" % "provided",
  "org.apache.spark"  % "spark-mllib_2.11" % "2.1.1" % "provided"
)

libraryDependencies ++= Seq(
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "org.scala-lang.modules" % "scala-parser-combinators_2.11" % "1.0.4",
  "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.4"
)

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "2.8.2"

libraryDependencies += "com.esotericsoftware" % "kryo" % "4.0.0"

//libraryDependencies += "com.github.alexandrnikitin" %% "bloom-filter" % "latest.release"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.6.1"
//libraryDependencies += "org.apache.directory.studio" % "org.apache.commons.lang" % "2.6"

//libraryDependencies  ++= Seq(
//  "org.scalanlp" %% "breeze" % "0.12",
//  "org.scalanlp" %% "breeze-natives" % "0.12"
//)

assemblyJarName in assembly := "myproject-assembly-1.0.jar"

test in assembly := {}
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "dpspc"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.4"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.4"
libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  "junit" % "junit" % "4.12" % Test,
  "com.github.haifengl" %% "smile-scala" % "2.6.0"
)

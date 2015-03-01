import sbt._
import Keys._

import sbtassembly.AssemblyKeys._
import sbtassembly._
import sbtassembly.AssemblyPlugin._

object OUTRBackupBuild extends Build {
  val baseSettings = Defaults.defaultSettings ++ Seq(
    version := "1.0.0-SNAPSHOT",
    organization := "com.outr.backup",
    scalaVersion := "2.11.5",
    libraryDependencies ++= Seq(
      Dependencies.PowerScalaProperty
    ),
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    resolvers ++= Seq("Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"),
    publishTo <<= version {
      (v: String) =>
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT"))
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishArtifact in Test := false,
    parallelExecution in Test := false,
    testOptions in Test += Tests.Argument("sequential")
  )

  private def createSettings(_name: String) = baseSettings ++ assemblySettings ++ Seq(name := _name)

  // Aggregator
  lazy val root = Project("root", file("."), settings = createSettings("outrbackup"))
    .settings(jarName in assembly <<= version map {
      (v: String) => "outrbackup-%s.jar".format(v)
    })
}

object Dependencies {
  private val PowerScalaVersion = "1.6.8-SNAPSHOT"

  val PowerScalaProperty = "org.powerscala" %% "powerscala-property" % PowerScalaVersion
}

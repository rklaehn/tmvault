import sbt._
import Keys._

object Build extends sbt.Build {

  val akka_actor = "com.typesafe.akka" %% "akka-actor" % "2.3.3"

  val leveldb = "org.iq80.leveldb" % "leveldb" % "0.7"

  val junit = "junit" % "junit" % "4.8" % "test"

  val junit_interface = "com.novocode" % "junit-interface" % "0.10" % "test"

  val debox = "org.spire-math" %% "debox" % "0.5.0"

  val scoverageSettings = scoverage.ScoverageSbtPlugin.instrumentSettings ++ (scoverage.ScoverageSbtPlugin.ScoverageKeys.highlighting := true)


  val buildSettings = Defaults.defaultSettings ++ scoverageSettings ++ Seq(
    organization := "rklaehn",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.11.1",
    libraryDependencies ++= Seq(junit, junit_interface),
    resolvers += "bintray/non" at "http://dl.bintray.com/non/maven",
    scalacOptions ++= Seq(
      "-deprecation",
      "-unchecked",
      "-optimize"
    )
  )

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = buildSettings
  ) aggregate (
    tmvault
    )

  lazy val tmvault = Project(
    id = "tmvault",
    base = file("tmvault"),
    settings = buildSettings ++ Seq(
      libraryDependencies ++= Seq(akka_actor, leveldb, debox)
    )
  )
}
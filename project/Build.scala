import sbt.Keys._
import sbt._

object Build extends Build {


  val appName = "async-couchbase"

  lazy val library = (project in file("."))
    .settings(
      name := appName,
      scalaVersion := "2.11.8",
      crossScalaVersions := Seq("2.11.8"),
      libraryDependencies ++= AppDependencies(),
      resolvers := Seq(
        Resolver.bintrayRepo("hmrc", "releases"),
        "typesafe-releases" at "http://repo.typesafe.com/typesafe/releases/"
      )
    )
}

private object AppDependencies {
  import play.core.PlayVersion

  val compile = Seq(
    "com.typesafe.play" %% "play" % PlayVersion.current % "provided",
    "com.couchbase.client" % "java-client" % "2.3.5",
    "io.reactivex" %% "rxscala" % "0.26.4"
  )

  val testScope: String = "test"

  val test = Seq(
    "org.scalatest" %% "scalatest" % "3.0.1" % testScope
  )

  def apply(): Seq[ModuleID] = compile ++ test
}



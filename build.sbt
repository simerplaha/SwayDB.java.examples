import sbt.Keys.{libraryDependencies, publishMavenStyle}
import sbt.url
import xerial.sbt.Sonatype._
import ReleaseTransformations._

val publishSettings = Seq[Setting[_]](
  sonatypeProfileName := "io.swaydb",
  publishMavenStyle := true,
  licenses := Seq("AGPL3" -> url("https://www.gnu.org/licenses/agpl-3.0.en.html")),
  publish := {},
  publishLocal := {},
  sonatypeProjectHosting := Some(GitHubHosting("simerplaha", "SwayDB.java", "simer.j@gmail.com")),
  developers := List(
    Developer(id = "simerplaha", name = "SwayDB", email = "simer.j@gmail.com", url = url("http://swaydb.io"))
  ),
  publishTo := sonatypePublishTo.value,
  crossPaths := false,
  publishConfiguration := publishConfiguration.value.withOverwrite(true),
  releaseIgnoreUntrackedFiles := true,
  releaseProcess := Seq[ReleaseStep](
    runClean,
    releaseStepCommandAndRemaining("+publishSigned"),
    releaseStepCommand("sonatypeReleaseAll"),
    pushChanges
  )
)

lazy val root =
  (project in file("."))
    .settings(
      organization := "io.swaydb",
      name := "swaydb-java",
      version := "0.8-beta.8.5",
      scalaVersion := scalaVersion.value
    )
    .settings(publishSettings)
    .settings(
      libraryDependencies ++=
        Seq(
          "io.swaydb" %% "swaydb" % "0.8-beta.8",
          "org.apache.commons" % "commons-lang3" % "3.8.1"
        )
    )

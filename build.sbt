
val useScalaVersion = "2.12.12"

lazy val publishSettings = Seq(
  publish / skip := false
)

lazy val scheduledJobFrameworkPublishSettings = Seq(
  publish / skip := false,
  crossScalaVersions := Seq("2.13.6", useScalaVersion)
)

libraryDependencies ++= Seq(
  Dependencies.akkaStream,
  Dependencies.akkaActor,
  Dependencies.akkaActorTyped,
  Dependencies.scaldi,
  Dependencies.logback,
  Dependencies.jodaTime,
  Dependencies.curator,
  Dependencies.curatorFramework
)

libraryDependencies ++= Dependencies.testScoped(
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % Versions.akka
)
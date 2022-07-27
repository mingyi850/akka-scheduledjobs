import sbt._

object Dependencies {
  import Versions._

  def testScoped(modules: ModuleID*): Seq[ModuleID] = modules map (_ % Test)
  def itScoped(modules: ModuleID*): Seq[ModuleID]   = modules map (_ % IntegrationTest)

  lazy val scalatest = "org.scalatest" %% "scalatest" % Tests.scalatest
  lazy val mockito   = "org.scalatestplus" %% "mockito-3-4" % Tests.mockito
  lazy val akkaStream = "com.typesafe.akka"      %% "akka-stream"           % Versions.akka
  lazy val scaldi = "org.scaldi"             %% "scaldi"                % Versions.scaldi
  lazy val circe = "io.circe"               %% "circe-generic"         % Versions.circe
  lazy val logback = "ch.qos.logback"          % "logback-classic"       % Versions.logback
  lazy val akkaActor =  "com.typesafe.akka"      %% "akka-actor"            % Versions.akka
  lazy val akkaActorTyped =  "com.typesafe.akka"      %% "akka-actor-typed" % Versions.akka
  lazy val openTracing = "io.opentracing"          % "opentracing-util" % Versions.openTracing
  lazy val mssqlJdbc = "com.microsoft.sqlserver" % "mssql-jdbc"       % Versions.mssqlJdbc
  lazy val jodaTime = "joda-time"               % "joda-time"        % Versions.joda
  lazy val curator                   = "org.apache.curator"             % "curator-recipes"               % Versions.vCurator
  lazy val curatorFramework          = "org.apache.curator"             % "curator-framework"             % Versions.vCurator
}

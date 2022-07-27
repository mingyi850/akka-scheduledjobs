resolvers ++= Seq(
  "Typesafe repository"       at "https://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Snapshots"    at "https://oss.sonatype.org/content/repositories/snapshots",
  "Github asbachb Release"    at "https://raw.github.com/asbachb/mvn-repo/master/releases",
  "jitpack"                   at "https://jitpack.io"
)

addDependencyTreePlugin


addSbtPlugin("com.thesamet"                    % "sbt-protoc"     % "1.0.4")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.9.7"

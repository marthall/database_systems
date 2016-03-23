name := "Project2"

version := "1.0"

artifactName := ((sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "Project2.jar")

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"



import java.io.File

import scoverage.ScoverageSbtPlugin.ScoverageKeys
import sbtassembly.AssemblyPlugin.autoImport._

organization := "com.vpon"

name := "ssp-etl-dedup"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.vpon"                  %% "vpon-mapping"              % "0.1-SNAPSHOT",
  "com.vpon"                  %% "ssp-etl-common"            % "0.1-SNAPSHOT",
  "org.apache.httpcomponents" %  "httpcore-nio"              % "4.2",
  "org.apache.httpcomponents" %  "httpcore"                  % "4.2",
  "com.typesafe"              %  "config"                    % "1.2.1",
  "ch.qos.logback"            %  "logback-classic"           % "1.1.3",
  "com.typesafe.akka"         %% "akka-actor"                % "2.3.11",
  "com.typesafe.akka"         %% "akka-cluster"              % "2.3.11",
  "com.typesafe.akka"         %% "akka-contrib"              % "2.3.11",
  "com.typesafe.akka"         %% "akka-remote"               % "2.3.11",
  "com.typesafe.akka"         %% "akka-slf4j"                % "2.3.11",
  "io.spray"                  %% "spray-can"                 % "1.3.2",
  "io.spray"                  %% "spray-json"                % "1.3.2",
  "io.spray"                  %% "spray-httpx"               % "1.3.2",
  "net.sandrogrzicic"         %% "scalabuff-compiler"        % "1.4.0"        % "compile",
  "net.sandrogrzicic"         %% "scalabuff-runtime"         % "1.4.0",
  "org.apache.commons"        %  "commons-lang3"             % "3.4",
  "com.github.scopt"          %% "scopt"                     % "3.3.0",
  "org.json4s"                %% "json4s-jackson"            % "3.2.10",
  "com.github.nscala-time"    %% "nscala-time"               % "2.2.0",
  "io.druid"                  %% "tranquility-core"          % "0.6.2",
  "com.twitter"               %% "util-core"                 % "6.25.0",
  "org.scalacheck"            %% "scalacheck"                % "1.12.2",
  "org.scalatest"             %% "scalatest"                 % "2.2.4",
  "com.typesafe.akka"         %% "akka-slf4j"                % "2.3.11"        % "test",
  "com.typesafe.akka"         %% "akka-testkit"              % "2.3.11"        % "test",
  "org.mockito"               %  "mockito-all"               % "1.9.5"         % "test",
  "org.scalatest"             %% "scalatest"                 % "2.2.1"         % "test",
  "net.manub"                 %% "scalatest-embedded-kafka"  % "0.3.0"         % "test"
)

resolvers ++= Seq(
  "Vpon Test Artifactory" at "http://192.168.101.29:8081/artifactory/vpon-test",
  "typesafe.com" at "http://repo.typesafe.com/typesafe/maven-releases/"
)

val protobuf = taskKey[Unit]("Compile protobuf files.")

def compileProtobuf(source: File,
                            sourceManaged: File,
                            classpath: Classpath,
                            javaHome: Option[File],
                            streams: TaskStreams,
                            cache: File): Seq[File] = {
  val input = source / "main" / "protobuf"
  val args = Seq()
  if (input.exists) {
    val mainClass = "net.sandrogrzicic.scalabuff.compiler.ScalaBuff"
    val output = source / "main" / "scala"
    val outputs = Seq(output)
    val cached = FileFunction.cached(cache / "scalabuff", FilesInfo.lastModified, FilesInfo.exists) {
      (in: Set[File]) => {
        println("compile protobuf files:", in.mkString(","))
        outputs.map { output =>
          IO.delete(output / "protobuf")
          IO.createDirectory(output)
          Fork.java(
            javaHome,
            List(
              "-cp", classpath.map(_.data).mkString(File.pathSeparator), mainClass,
              "--scala_out=" + output.toString,
              "--generate_json_method"
            ) ++ args.toSeq ++ in.toSeq.map(_.toString),
            streams.log
          )

          (output ** ("*.scala")).get.toSet
        }.head
      }
    }
    cached((input ** "*.proto").get.toSet).toSeq
  } else Nil
}

scalacOptions += "-deprecation"

javaOptions in run ++= Seq("-verbosegc", "-XX:+PrintGCDetails", "-Xloggc:gc.log")

// Execute everything serially (including compilation and tests)
parallelExecution := false

// Keep resolution cache files to speed up build process
updateOptions := updateOptions.value.withCachedResolution(true)

mainClass := Some("com.vpon.ssp.report.dedup.Main")

test in assembly := {}

ScoverageKeys.coverageExcludedPackages := "<empty>"
ScoverageKeys.coverageMinimum := 80
ScoverageKeys.coverageFailOnMinimum := false

protobuf := {
  compileProtobuf(sourceDirectory.value, sourceManaged.value, (managedClasspath in Compile).value, javaHome.value, streams.value, cacheDirectory.value)
}

assemblyMergeStrategy in assembly := {
  // Classes
  case n if n.startsWith("javax/activation/")               => MergeStrategy.first
  case n if n.startsWith("javax/annotation/")               => MergeStrategy.first
  case n if n.startsWith("javax/mail/")                     => MergeStrategy.first
  case n if n.startsWith("javax/servlet/")                  => MergeStrategy.first
  case n if n.startsWith("javax/transaction/")              => MergeStrategy.first
  case n if n.startsWith("org/objectweb/asm/")              => MergeStrategy.first
  case n if n.startsWith("org/slf4j/impl")                  => MergeStrategy.first
  case n if n.startsWith("javax/xml/namespace")             => MergeStrategy.last

  // Package Dependency Messages
  case n if n.startsWith("META-INF/maven/")                 => MergeStrategy.discard
  case n if n.startsWith("META-INF/plexus/")                => MergeStrategy.discard
  case n if n.startsWith("META-INF/sisu/")                  => MergeStrategy.discard
  case n if n.startsWith("META-INF/DEPENDENCIES")           => MergeStrategy.discard

  // Signature files
  case n if n.startsWith("META-INF/BCKEY.DSA")              => MergeStrategy.discard
  case n if n.startsWith("META-INF/BCKEY.SF")               => MergeStrategy.discard
  case n if n.startsWith("META-INF/DEV.DSA")                => MergeStrategy.discard
  case n if n.startsWith("META-INF/DEV.SF")                 => MergeStrategy.discard
  case n if n.startsWith("META-INF/ECLIPSEF.RSA")           => MergeStrategy.discard
  case n if n.startsWith("META-INF/ECLIPSEF.SF")            => MergeStrategy.discard
  case n if n.startsWith("META-INF/eclipse.inf")            => MergeStrategy.discard
  case n if n.startsWith("META-INF/MANIFEST.MF")            => MergeStrategy.discard

  // License files
  case n if n.startsWith("META-INF/mailcap")                => MergeStrategy.discard
  case n if n.startsWith("META-INF/NOTICE")                 => MergeStrategy.discard
  case n if n.startsWith("META-INF/INDEX.LIST")             => MergeStrategy.discard
  case n if n.startsWith("META-INF/LICENSE")                => MergeStrategy.discard
  case n if n.startsWith("about_files/")                    => MergeStrategy.discard
  case n if n.startsWith("about.html")                      => MergeStrategy.discard
  case n if n.startsWith("NOTICE")                          => MergeStrategy.discard
  case n if n.startsWith("LICENSE")                         => MergeStrategy.discard
  case n if n.startsWith("LICENSE.txt")                     => MergeStrategy.discard
  case n if n.startsWith("rootdoc.txt")                     => MergeStrategy.discard
  case n if n.startsWith("readme.html")                     => MergeStrategy.discard
  case n if n.startsWith("readme.txt")                      => MergeStrategy.discard
  case n if n.startsWith("license.html")                    => MergeStrategy.discard

  // Service provider configuration files
  case n if n.startsWith("META-INF/services/")              => MergeStrategy.first

  // System default properties
  case n if n.startsWith("META-INF/mimetypes.default")      => MergeStrategy.discard
  case n if n.startsWith("application.conf")                => MergeStrategy.discard
  case n if n.startsWith("application.properties")          => MergeStrategy.discard
  case n if n.startsWith("application.json")                => MergeStrategy.discard
  case n if n.startsWith("reference.conf")                  => MergeStrategy.concat
  case n if n.startsWith("library.properties")              => MergeStrategy.discard
  case n if n.startsWith("plugin.properties")               => MergeStrategy.concat
  case n if n.startsWith("mime.types")                      => MergeStrategy.discard
  case n if n.startsWith("logback.xml")                     => MergeStrategy.discard

  case n => MergeStrategy.deduplicate
}

excludeFilter in unmanagedSources in Compile := HiddenFileFilter

publishTo := {
  val artifactory = "http://192.168.101.29:8081/artifactory/vpon-"
  if (isSnapshot.value)
    Some("Vpon snapshots" at artifactory + "test")
  else
    Some("Vpon releases" at artifactory + "test")
}

publishMavenStyle := true

credentials += Credentials("Artifactory Realm", "192.168.101.29", "vpon-test", "vpon-test")
//credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")
//  realm=Artifactory Realm
//  host=192.168.101.29
//  user=vpon-test
//  password=vpon-test
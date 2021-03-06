import sbt._
import Keys._
import com.github.siasia._


object BuildSettings {
  val buildOrganization = "org.riedelcastro"
  val buildVersion = "0.1-SNAPSHOT"
  val buildScalaVersion = "2.9.0-1"

  val buildSettings = Defaults.defaultSettings ++ Seq(
    organization := buildOrganization,
    version := buildVersion,
    scalaVersion := buildScalaVersion,
    scalacOptions ++= Seq("-unchecked", "-deprecation"),
    shellPrompt := ShellPrompt.buildShellPrompt
  ) ++ WebPlugin.webSettings
}

// Shell prompt which show the current project,
// git branch and build version
object ShellPrompt {
  object devnull extends ProcessLogger {
    def info(s: => String) {}

    def error(s: => String) {}

    def buffer[T](f: => T): T = f
  }
  def currBranch = {
    try {
      (("git status -sb" lines_! devnull headOption)
        getOrElse "-" stripPrefix "## ")
    }
    catch {
      case _ => "-"
    }
  }


  val buildShellPrompt = {
    (state: State) => {
      val currProject = Project.extract(state).currentProject.id
      "%s:%s:%s> ".format(
        currProject, currBranch, BuildSettings.buildVersion
      )
    }
  }
}

object Resolvers {
  val allResolvers = Seq(
    DefaultMavenRepository,
    "IESL third party" at "http://iesl.cs.umass.edu:8081/nexus/content/repositories/thirdparty/",
    "IESL snapshots" at "http://iesl.cs.umass.edu:8081/nexus/content/repositories/snapshots",
    "IESL releases" at "http://iesl.cs.umass.edu:8081/nexus/content/repositories/releases",
    "EBI" at "http://www.ebi.ac.uk/~maven/m2repo",
    "Novus Releases" at "http://repo.novus.com/releases/",
    "Novus Snapshots" at "http://repo.novus.com/snapshots/",
    "Akka releases" at "http://akka.io/repository",
    "Local Ivy Repository" at "file://" + Path.userHome.absolutePath + "/.ivy/local/default",
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    "OpenNLP Maven Repo" at "http://opennlp.sourceforge.net/maven2"
  )
}

object Dependencies {

  val liftVersion = "2.4-M3"

  val lift = Seq(
    "net.liftweb" %% "lift-webkit" % liftVersion % "compile->default",
    "net.liftweb" %% "lift-mapper" % liftVersion % "compile->default",
    "net.liftweb" %% "lift-wizard" % liftVersion % "compile->default")

  val liftDeps = Seq(
    "junit" % "junit" % "4.5" % "test->default",
    "org.mortbay.jetty" % "jetty" % "6.1.25" % "jetty",
    "javax.servlet" % "servlet-api" % "2.5" % "provided->default",
    "com.h2database" % "h2" % "1.2.138",
    "ch.qos.logback" % "logback-classic" % "0.9.26" % "compile->default"
  )

  val others = Seq(
    "org.riedelcastro.nurupo" %% "nurupo" % "0.1-SNAPSHOT",
    "se.scalablesolutions.akka" % "akka-actor" % "1.1.3",
    "cc.refectorie.proj.factorieie" % "factorieie" % "1.3.1-SNAPSHOT",
    "com.novus" %% "salat-core" % "0.0.8-SNAPSHOT",
    "edu.stanford" % "stanford-corenlp-faust" % "2011-07-22",
    "edu.stanford" % "stanford-corenlp-faust-models" % "2011-06-19",
    "xom" % "xom" % "1.2.5",
    "org.apache.opennlp" % "opennlp-tools" % "1.5.1-incubating",
    "org.apache.opennlp" % "opennlp-tools-models" % "1.5",
    "org.scala-tools.sbt" % "launcher-interface_2.8.1" % "0.10.1" % "provided",
    //I don't know why this is necessary---it's already in nurupo. It only seems required
    //in sbt launchers
    "log4j" % "log4j" % "1.2.16"
  )

  val allDeps = lift ++ liftDeps ++ others
}

object ClusterHubBuild extends Build {

  import Resolvers._
  import Dependencies._
  import BuildSettings._

  lazy val clusterhub = Project(
    "clusterhub",
    file("."),
    settings = buildSettings ++ Seq(
      resolvers ++= allResolvers,
      resolvers <+= sbtResolver.identity,
      libraryDependencies ++= allDeps
    )
  )

}
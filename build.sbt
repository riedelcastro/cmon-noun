// Set the project name to the string 'My Project'
name := "clusterhub"

// The := method used in Name and Version is one of two fundamental methods.
// The other method is <<=
// All other initialization methods are implemented in terms of these.
version := "0.1-SNAPSHOT"

resolvers ++= Seq(

    "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
    "IESL third party" at "http://iesl.cs.umass.edu:8081/nexus/content/repositories/thirdparty/",
    "IESL snapshots" at "http://iesl.cs.umass.edu:8081/nexus/content/repositories/snapshots",
    "IESL releases" at "http://iesl.cs.umass.edu:8081/nexus/content/repositories/releases",
    "EBI" at "http://www.ebi.ac.uk/~maven/m2repo",
    "Novus Releases" at "http://repo.novus.com/releases/",
    "Novus Snapshots" at "http://repo.novus.com/snapshots/",
    "Akka releases" at "http://akka.io/repository",

)

// Add multiple dependencies
libraryDependencies ++= Seq(
    "org.riedelcastro.nurupo" % "nurupo" % "0.1-SNAPSHOT",
    "se.scalablesolutions.akka" % "akka-actor" % "1.0",
    "net.liftweb" % "lift-mapper_${scala.version}" % "2.4-M3",
    "junit" % "junit" % "4.8" % "test",
    "net.databinder" %% "dispatch-google" % "0.7.8",
    "net.databinder" %% "dispatch-meetup" % "0.7.8"
)

// Exclude backup files by default.  This uses ~=, which accepts a function of
//  type T => T (here T = FileFilter) that is applied to the existing value.
// A similar idea is overriding a member and applying a function to the super value:
//  override lazy val defaultExcludes = f(super.defaultExcludes)
//
defaultExcludes ~= (filter => filter || "*~")
/*  Some equivalent ways of writing this:
defaultExcludes ~= (_ || "*~")
defaultExcludes ~= ( (_: FileFilter) || "*~")
defaultExcludes ~= ( (filter: FileFilter) => filter || "*~")
*/

// Use the project version to determine the repository to publish to.
publishTo <<= version { (v: String) =>
  if(v endsWith "-SNAPSHOT")
    Some(ScalaToolsSnapshots)
  else
    Some(ScalaToolsReleases)
}
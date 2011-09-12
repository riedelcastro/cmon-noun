package bootstrap.liftweb

import _root_.net.liftweb.util._
import _root_.net.liftweb.common._
import _root_.net.liftweb.http._
import _root_.net.liftweb.http.provider._
import _root_.net.liftweb.sitemap._
import _root_.net.liftweb.sitemap.Loc._
import _root_.net.liftweb.mapper.{DB, Schemifier, DefaultConnectionIdentifier, StandardDBVendor}
import _root_.org.riedelcastro.cmonnoun.model._
import org.riedelcastro.cmonnoun.snippet.ClusterParam


/**
 * A class that's instantiated early and run.  It allows the application
 * to modify lift's environment
 */
class Boot {
  def boot {

//    StatelessJson.init()
    //LiftRules.dispatch.append(AsyncRest)

    //LiftRules.htmlProperties.default.set((r: Req) => new Html5Properties(r.userAgent))

    // where to search snippet
    LiftRules.addToPackages("org.riedelcastro.cmonnoun")
//    Schemifier.schemify(true, Schemifier.infoF _, User)

    val menu = Menu.param[String]("Tasks", "Tasks",
      s => Full(s),
      pi => pi) / "task"

    val cluster = Menu.param[String]("Clusters", "Clusters",
      s => Full(s),
      pi => pi) / "cluster"

    val corpus = Menu.param[String]("Corpora", "Corpora",
      s => Full(s),
      pi => pi) / "corpus"

    val entities = Menu.param[String]("Entities", "Entities",
      s => Full(s),
      pi => pi) / "entities"

    val admin = Menu("Admin") / "admin"


    // Build SiteMap
    def sitemap() = SiteMap(
      menu, cluster, corpus, entities , admin,
      Menu("Home") / "index" >> User.AddUserMenusAfter, // Simple menu form
      // Menu with special Link
      Menu(Loc("Static", Link(List("static"), true, "/static/index"),
        "Static Content")))

    LiftRules.setSiteMapFunc(() => User.sitemapMutator(sitemap()))

    /*
     * Show the spinny image when an Ajax call starts
     */
    LiftRules.ajaxStart =
      Full(() => LiftRules.jsArtifacts.show("ajax-loader").cmd)

    /*
     * Make the spinny image go away when it ends
     */
    LiftRules.ajaxEnd =
      Full(() => LiftRules.jsArtifacts.hide("ajax-loader").cmd)

    LiftRules.early.append(makeUtf8)

    LiftRules.loggedInTest = Full(() => User.loggedIn_?)

//    S.addAround(DB.buildLoanWrapper)
  }

  /**
   * Force the request to be UTF-8
   */
  private def makeUtf8(req: HTTPRequest) {
    req.setCharacterEncoding("UTF-8")
  }
}

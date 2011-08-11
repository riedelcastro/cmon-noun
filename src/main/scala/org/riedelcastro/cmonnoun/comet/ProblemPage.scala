package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.CometActor
import net.liftweb.common.Full
import net.liftweb.sitemap.Menu
import xml.Text

/**
 * @author sriedel
 */
class ProblemPage(val problemName: String) extends CometActor {

  val menu = Menu.param[String]("Problem", "Problem",s => Full(s),pi => pi) / "problem"

  def render = Text("Blah")
}
package org.riedelcastro.cmonnoun.snippet

import net.liftweb.util._
import Helpers._

/**
 * @author sriedel
 */
class ProblemPage(problemName: String) {

  def render = {
    "#problemName *" #> problemName
  }
}
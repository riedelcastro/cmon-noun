package org.riedelcastro.cmonnoun.snippet

import net.liftweb.util._
import Helpers._
import org.riedelcastro.cmonnoun.comet.Controller

/**
 * @author sriedel
 */
class ProblemPage(problemName: String) {



  def render = {
    "#problemName *" #> problemName
  }
}
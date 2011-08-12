package org.riedelcastro.cmonnoun.snippet

import net.liftweb.util._
import Helpers._

/**
 * @author sriedel
 */
class Test(param:String) {

   def render = {
    "#comet [name]" #> param
   }
}
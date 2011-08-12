package org.riedelcastro.cmonnoun.comet

import net.liftweb.http.CometActor

/**
 * @author sriedel
 */
class TestComet extends CometActor {
  def render = {
    "#param *" #> name.get
  }
}
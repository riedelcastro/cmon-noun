package org.riedelcastro.cmonnoun.clusterhub

import org.riedelcastro.nurupo.{Util, Config}

/**
 * @author sriedel
 */
object NeoConf
  extends Config(Util.getStreamFromFileOrClassPath(System.getProperty("prop", "neo-millerntor.properties")))
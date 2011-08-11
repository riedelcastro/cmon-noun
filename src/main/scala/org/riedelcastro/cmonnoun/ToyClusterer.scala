package org.riedelcastro.cmonnoun

import io.Source
import org.riedelcastro.nurupo.Util
import java.io.{PrintStream, InputStream}

/**
 * @author sriedel
 */
object ToyClusterer {

  def loadFreqsFromTxt(is:InputStream, skip:Int = 0) = {
    val freqSource = Source.fromInputStream(is)
    val freqs = freqSource.getLines().drop(skip).map(_.split("\t")).map(s => s(0) -> s(1).toDouble).toMap
    freqs
  }

  def main(args: Array[String]) {
    val freqs = loadFreqsFromTxt(Util.getStreamFromClassPathOrFile("data/stats/he-is.txt"),1)
    val totals = loadFreqsFromTxt(Util.getStreamFromClassPathOrFile("data/stats/bing_priors_filtered.txt"))
    val out = new PrintStream("data/stats/he-is-likely.txt")
    val likelies = freqs.map({case (key,value) => key -> (value / totals(key))}).toSeq
    val sorted = likelies.sortBy(_._2)
    for ((key,value) <- sorted) out.println("%s\t%e".format(key,value))
    out.close()
  }
}
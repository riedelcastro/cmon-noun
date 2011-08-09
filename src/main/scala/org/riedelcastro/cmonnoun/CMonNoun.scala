package org.riedelcastro.cmonnoun

import java.net.URL
import java.io.InputStream
import io.Source
import util.parsing.json.JSON
import org.riedelcastro.nurupo.Util

/**
 * @author sriedel
 */
object CMonNoun {

  def getTotalCount(json: List[Any]): Option[Double] = {
    val searchResponse = json.find({
      case ("SearchResponse", _) => true
      case _ => false
    }).map({
      case (_, l: List[_]) => l
    })
    val web = searchResponse.get.find({
      case ("Web", _) => true
      case _ => false
    }).map({
      case (_, l: List[_]) => l
    })
    val total = web.get.find({
      case ("Total", _) => true
      case _ => false
    }).map({
      case (_, s: Double) => s
    })
    total
  }

  def queryCount(queryRaw:String, appID:String):Option[Double] = {
    val queryTemplate = "http://api.bing.net/json.aspx?AppId=%s&Version=2.2" +
      "&Market=en-US&Query=%s&Sources=web+spell&Web.Count=1&JsonType=raw"
    val query = queryRaw.replaceAll(" ", "+")
    val httpQuery = queryTemplate.format(appID, query)
    val (_, stream) = Http.request(httpQuery)
    val response = Source.fromInputStream(stream).getLines().mkString("\n")
    val json = JSON.parse(response)
    getTotalCount(json.get)
  }

  def main(args: Array[String]) {
    val appID = args(0)
    val nounFile = Source.fromInputStream(Util.getStreamFromClassPathOrFile("nounlist.txt")).getLines().take(10)
    for (noun <- nounFile){
      val query = "\"%s for Microsoft\"".format(noun)
      val count = queryCount(query,appID).get
      println("%-15s %f".format(noun,count))
    }
  }

}

object Http {
  def request(urlString: String): (Boolean, InputStream) =
    try {
      val url = new URL(urlString)
      val body = url.openStream
      (true, body)
    }
    catch {
      case ex: Exception => {
        println(ex)
        (false, null)
      }
    }
}
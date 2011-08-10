package org.riedelcastro.cmonnoun

import java.net.URL
import io.Source
import util.parsing.json.JSON
import org.riedelcastro.nurupo.Util
import collection.mutable.HashSet
import java.io.{PrintStream, File, InputStream}

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

  val vowels = Set('a','e','o','u','i')

  case class Pattern(text: String) {
    def inject(value: String) = {
      val xReplaced = text.replaceAll("X", value)
      val indef = if (vowels(value.head)) "an" else "a"
      val indefReplaced = xReplaced.replaceAll("INDEF",indef)
      "\"" + indefReplaced + "\""
    }
  }

  val worksAs = Pattern("works as X for")
  val playsAs = Pattern("plays as X for")
  val hiredAs = Pattern("hired as X for")
  val makeMoney = Pattern("money as an X for")
  val heIsIn = Pattern("he is INDEF X for")



  def queryCount(queryRaw: String, appID: String): Option[Double] = {
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
    val nounFile = Source.fromInputStream(Util.getStreamFromClassPathOrFile("data/nounlist.txt")).getLines().take(200)
    for (noun <- nounFile) {
      val query = heIsIn.inject(noun)
      for (total <- queryCount(noun, appID); count <- queryCount(query, appID))
        println("%-15s %f %f %f".format(noun, count / total, count, total))
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

object FilterWordNetEntity {
  def main(args: Array[String]) {
    val entities = Source.fromInputStream(Util.getStreamFromClassPathOrFile("wordnet-entity.txt")).getLines()
    val nouns = new HashSet[String]
    for (entity <- entities) {
      val split = entity.split(" ")
      val last = split.last
      if (last.head.isLower) nouns += last
    }
    val out = new PrintStream(new File(args(0)))
    for (noun <- nouns.toSeq.sorted) out.println(noun)
    out.close()

  }
}
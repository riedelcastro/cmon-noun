package org.riedelcastro.cmonnoun

import java.net.URL
import io.Source
import util.parsing.json.JSON
import collection.mutable.HashSet
import java.io.{PrintStream, File, InputStream}
import org.riedelcastro.nurupo.{HasLogger, Counting, Util}

/**
 * @author sriedel
 */
object CMonNoun extends HasLogger {

  def getTotalCount(json: List[Any]): Option[Double] = {
    try {
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
    catch {
      case _ => None
    }
  }

  val vowels = Set('a', 'e', 'o', 'u', 'i')

  case class Pattern(text: String) {
    def inject(value: String) = {
      val xReplaced = text.replaceAll("X", value)
      val indef = if (vowels(value.head)) "an" else "a"
      val indefReplaced = xReplaced.replaceAll("INDEF", indef)
      "\"" + indefReplaced + "\""
    }
  }

  val worksAs = Pattern("works as X for")
  val playsAs = Pattern("plays as X for")
  val hiredAs = Pattern("hired as X for")
  val makeMoney = Pattern("money as an X for")
  val heIs = Pattern("he is INDEF X for")


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
    println(args.mkString("\n"))
    val appID = args(0)
    val nouns = args(1)
    val dest = args(2)
    val pattern = Pattern(args(3))
    writeoutPatternStats(nouns, pattern, new File(dest), appID)
  }

  def writeoutPatternStats(nouns: String, pattern: Pattern, dest: File, appID: String) {
    val out = new PrintStream(dest)
    out.println(pattern.text)
    val nounFile = Source.fromInputStream(Util.getStreamFromClassPathOrFile(nouns)).getLines()
    val counter = new Counting(20, count => logger.info("Processed %d nouns".format(count)))
    for (noun <- counter(nounFile)) {
      val query = heIs.inject(noun)
      for (total <- queryCount(noun, appID); count <- queryCount(query, appID))
        out.println("%-15s %f %f".format(noun, count, total))
    }
    out.close()
  }
}

object GetBingPriors extends HasLogger {
  def main(args: Array[String]) {
    val appID = args(0)
    val out = new PrintStream("data/stats/bing_priors.txt")
    val nounFile = Source.fromInputStream(Util.getStreamFromClassPathOrFile("data/combined.txt")).getLines()
    val counter = new Counting(100, count => logger.info("Processed %d nouns".format(count)))
    for (noun <- counter(nounFile)) {
      val total = CMonNoun.queryCount(noun, appID).getOrElse(-1.0)
      out.println("%s\t%f".format(noun, total))
    }
    out.close()
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

object FilterByPrior {
  def main(args: Array[String]) {
    val lines = Source.fromFile(args(0)).getLines()
    val out = new PrintStream(args(1))
    val thresh = args(2).toDouble
    for (line <- lines) {
      val Array(noun, prior) = line.split("\t")
      if (prior.toDouble >= thresh) {
        out.println(line)
      }
    }
    out.close()
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
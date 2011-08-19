package org.riedelcastro.cmonnoun.clusterhub

import akka.actor.{Actor, ActorRef}
import org.riedelcastro.nurupo.HasLogger
import org.bson.types.ObjectId
import com.mongodb.casbah.Imports._
import org.riedelcastro.cmonnoun.clusterhub.ClusterManager.{SortByContent, SortByProb, Dict, DictEntry}
import com.mongodb.casbah.commons.MongoDBObject
import collection.mutable.{HashSet, HashMap, ArrayBuffer}
import java.util.Random
import org.riedelcastro.cmonnoun.clusterhub.CorpusManager.TokenSpec

/**
 * @author sriedel
 */
trait ClusterPersistence extends MongoSupport {

  this: ClusterManager =>

  protected val extractors = new ArrayBuffer[FieldExtractor]
  protected val dicts = new ArrayBuffer[ClusterManager.Dict]


  private val reserved = Set("_id", "content", "prob", "edit")

  def toMongoFieldName(name: String): String = "_" + name
  def fromMongoFieldName(fieldName: String): String = fieldName.drop(1)

  def storeDict(name:String, entries:TraversableOnce[DictEntry]) {
    for (s <- state) {
      val dictList = collFor(s.clusterId, "dicts")
      dictList += MongoDBObject("name" -> name)
      val coll = collFor(s.clusterId, "dict_" + name)
      val dict = for (entry <- entries) yield {
        val dbo = MongoDBObject("key" -> entry.key, "score" -> entry.score)
        coll += dbo
        entry.key -> entry.score
      }
      dicts += Dict(name,dict.toMap)
    }
  }

  def loadDictNames():Seq[String] = {
    val result = for (s <- state) yield {
      collFor(s.clusterId, "dicts").map(_.as[String]("name")).toSeq
    }
    result.getOrElse(Seq.empty)
  }

  def loadDict(name:String):ClusterManager.Dict = {
    val pairs = for (s <- state) yield {
      val coll = collFor(s.clusterId, "dict_" + name)
      for (dbo <- coll.find()) yield {
        val key = dbo.as[String]("key")
        val score = dbo.getAs[Double]("score").getOrElse(1.0)
        key -> score
      }
    }
    Dict(name,pairs.map(_.toMap).getOrElse(Map.empty))
  }

  def loadDicts() {
    for (name <- loadDictNames()){
      dicts += loadDict(name)
    }
  }

  def collForRowsOfCluster(name: String): MongoCollection = {
    val coll = collFor(name, "rows")
    coll.ensureIndex(MongoDBObject("doc" -> 1, "sentence" -> 1))
    coll.ensureIndex(MongoDBObject("content" -> 1))
    coll
  }
  def addRow(row: Row) {
    for (s <- state) {
      val name = s.clusterId
      val coll = collForRowsOfCluster(name)
      val basic = List(
        "_id" -> row.id,
        "content" -> row.instance.content
      )
      val fields = row.instance.fields.toList.map({case (k, v) => toMongoFieldName(k) -> v})
      val dbo = MongoDBObject(basic ++ fields)
      if (row.label.edit != 0.5) {
        dbo.put("edit", row.label.edit)
      }
      if (row.label.prob != 0.5) {
        dbo.put("prob", row.label.prob)
      }
      coll += dbo
    }
  }

  def loadRows(): TraversableOnce[Row] = {
    val opt = for (s <- state) yield {
      val coll = collForRowsOfCluster(s.clusterId)
      for (dbo <- coll.find()) yield {
        loadRow(dbo)
      }
    }
    opt.getOrElse(Seq.empty)
  }

  def loadRow(dbo: DBObject): Row = {
    val id = dbo._id.get
    val content = dbo.as[String]("content")
    val prob = dbo.getAs[Double]("prob").getOrElse(0.5)
    val edit = dbo.getAs[Double]("edit").getOrElse(0.5)
    val fields = dbo.filterKeys(!reserved(_))
    val docId = dbo.getAs[String]("doc").getOrElse("_own")
    val sentenceIndex = dbo.getAs[Int]("sentence").getOrElse(0)
    val tokenIndex = dbo.getAs[Int]("token").getOrElse(0)
    val renamed = fields.map({case (k, v) => fromMongoFieldName(k) -> v})
    val label = RowLabel(prob, edit)
    val instance = RowInstance(content, renamed.toMap)
    val spec = TokenSpec(docId, sentenceIndex, tokenIndex)
    Row(instance, label, id, spec)
  }
  def randomRows(): TraversableOnce[Row] = {
    val opt = for (s <- state) yield {
      val coll = collForRowsOfCluster(s.clusterId)
      val size = coll.size
      val taken = new HashSet[ObjectId]
      val takenRows = new ArrayBuffer[Row]
      while (taken.size <= 10 && taken.size < size) {
        val randomIndex = scala.util.Random.nextInt(size)
        var dbo:DBObject = coll.find().skip(randomIndex).next()
        var j = randomIndex + 1
        //search forward until something is found
        while (taken(dbo._id.get) && j < size) {
          dbo = coll.find().skip(j).next()
          j += 1
        }
        //search backward if nothing can be found
        var i = randomIndex - 1
        while (taken(dbo._id.get) && i >= 0) {
          dbo = coll.find().skip(i).next()
          i -= 1
        }
        taken += dbo._id.get
        takenRows += loadRow(dbo)
      }
      takenRows
    }
    opt.getOrElse(Seq.empty)
  }


  def query(query:ClusterManager.Query):TraversableOnce[Row] = {
    val opt = for (s <- state) yield {
      val coll = collForRowsOfCluster(s.clusterId)
      val asc = if (query.ascending) 1 else -1
      val sortOn = query.sorting match {
        case SortByProb => "prob"
        case SortByContent => "content"
      }
      val q = MongoDBObject("content" -> MongoDBObject("$regex" -> query.content))
      val sort = MongoDBObject(sortOn -> asc)
      for (dbo <- coll.find(q,null).sort(sort).skip(query.from).limit(query.batchSize)) yield {
        loadRow(dbo)
      }
    }
    opt.getOrElse(Seq.empty)
  }

  def evaluateSpecOnRows(spec: FieldExtractor) {
    for (s <- state) {
      val coll = collFor(s.clusterId, "rows")
      for (row <- loadRows()) {
        val q = MongoDBObject("_id" -> row.id)
        val set = MongoDBObject("$set" -> MongoDBObject(
          toMongoFieldName(spec.spec.name) -> spec.extract(row.instance.content)
        ))
        coll.update(q, set)
      }

    }
  }

  def addSpec(spec: FieldSpec) {
    for (s <- state) {
      val coll = collFor(s.clusterId, "specs")
      val extractor = spec match {
        case regexSpec@RegExFieldSpec(name, regex) =>
          coll += MongoDBObject("type" -> "regex", "name" -> name, "regex" -> regex)
          new RegexExtractor(regexSpec)
        case dictSpec@DictFieldSpec(name, dictName,gaussian) =>
          coll += MongoDBObject(
            "type" -> "dict",
            "name" -> name,
            "dictName" -> dictName,
            "gaussian" -> gaussian)
          val map = dicts.find(_.name == dictName).get.map
          gaussian match {
            case true => new DictScoreExtractor(dictSpec,map)
            case false => new DictExtractor(dictSpec,map)
          }


      }
      extractors += extractor
      evaluateSpecOnRows(extractor)
    }
  }



  def loadSpecs() {
    for (s <- state) {
      for (dbo <- collFor(s.clusterId, "specs").find()){
        val name = dbo.as[String]("name")
        dbo.as[String]("type") match {
          case "regex" =>
            val regex = dbo.as[String]("regex")
            extractors += new RegexExtractor(RegExFieldSpec(name,regex))
          case "dict" =>
            val dictName = dbo.as[String]("dictName")
            val map = dicts.find(_.name == dictName).get.map
            extractors += new DictExtractor(DictFieldSpec(name,dictName, dbo.as[Boolean]("gaussian")),map)
        }
      }
    }
  }


  def editLabel(id: ObjectId, value: Double) {
    setRowField(id, "edit", value)
  }

  def setProb(id: ObjectId, value: Double) {
    setRowField(id, "prob", value)
  }

  def setRowField(id: ObjectId, name: String, value: Any) {
    for (s <- state) {
      val coll = collFor(s.clusterId, "rows")
      val q = MongoDBObject("_id" -> id)
      val set = MongoDBObject("$set" -> MongoDBObject(name -> value))
      coll.update(q, set)
    }

  }




}















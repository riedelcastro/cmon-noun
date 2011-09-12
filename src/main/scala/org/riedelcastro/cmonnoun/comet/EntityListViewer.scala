package org.riedelcastro.cmonnoun.comet

import org.riedelcastro.nurupo.HasLogger
import xml.Text
import akka.actor.ActorRef
import org.riedelcastro.cmonnoun.comet.EntityListViewer.SetParams
import net.liftweb.http.SHtml
import org.riedelcastro.cmonnoun.clusterhub.{BinaryClusterService, EntityMentionAlignmentService, ServiceRegistry, EntityService}

/**
 * @author sriedel
 */
class EntityListViewer extends CallMailboxFirst with HasLogger {
  def cometType = "entities"

  import EntityService._

  private var entityService: Option[ActorRef] = None
  private var alignmentService: Option[ActorRef] = None
  private var entityServiceName: Option[String] = None
  private var entityQuery: Option[Query] = Some(Query(EntityService.All, 0, 10))
  private var currentEntities: Seq[EntityInformation] = Seq.empty
  private var nameQuery:Option[String] = None
  private var clusterNames:Option[String] = None
  private var clusterServices:Option[Seq[ClusterServiceRef]] = None

  case class ClusterServiceRef(name:String, service:ActorRef)
  case class ClusterAnnotation(name:String, prob:Double, label:Option[Double]) {
    override def toString = "%s:%4.3f".format(name,prob)
  }

  case class EntityInformation(entity: Entity,
                               mentions: Seq[Any] = Seq.empty,
                               clusters:Seq[ClusterAnnotation] = Seq.empty)


  override def lowPriority = {
    case SetParams(name, alignmentServiceName) =>
      import ServiceRegistry._

      entityServiceName = Some(name)
      for (Service(name, service) <- Global.entityServiceRegistry !! GetOrCreateService(name)) {
        entityService = Some(service)
        updateAll()
      }
      for (alignName <- alignmentServiceName) {
        for (Service(_, service) <- Global.entityMentionAlignmentServiceRegistry !! GetOrCreateService(alignName)) {
          alignmentService = Some(service)
        }
      }
      reRender(false)
  }

  def updateEntities() {
    for (service <- entityService; query <- entityQuery) {
      for (Entities(ents) <- service !! query) {
        currentEntities = ents.toSeq.map(EntityInformation(_))
      }
    }
  }

  def processAlignments(a: Map[EntityMentionAlignmentService.EntityMentionId, EntityMentionAlignmentService.EntityId]) {
    currentEntities = for (ent <- currentEntities) yield {
      val mentionIds = a.filter(_._2 == ent.entity.id).map(_._1)
      ent.copy(mentions = mentionIds.toSeq)
    }
  }

  def updateMentionInformation() {
    import EntityMentionAlignmentService._
    for (s <- alignmentService) {
      val ids = currentEntities.map(_.entity.id)
      for (Alignments(a) <- s !! GetAlignments(ids.toStream)) {
        processAlignments(a)
      }
    }
  }

  def updateClusterInformation() {
    import BinaryClusterService._
    for (refs <- clusterServices){
      val ids = currentEntities.map(_.entity.id)
      for (ref <- refs){
        for (Instances(instances) <- ref.service !! GetInstances(ids.toStream)) {
          val id2instance = instances.map(i => i.id -> i).toMap
          currentEntities = for (ent <- currentEntities) yield {
            val newEnt = for (instance <- id2instance.get(ent.entity.id)) yield {
              ent.copy(clusters = ent.clusters :+ ClusterAnnotation(ref.name,instance.prob,instance.label))
            }
            newEnt.getOrElse(ent)
          }
        }
      }
    }
  }


  def freebaseEntityTypeFilter(types: Seq[String]) = {
    for (t <- types; if (!t.startsWith("/user") && !t.startsWith("/base"))) yield
      t.substring(t.lastIndexOf("/") + 1)
  }

  def updateAll() {
    updateEntities()
    updateMentionInformation()
    updateClusterInformation()
  }

  def render = {
    val stats = Seq(
      ".entity_service_name" #> Text(entityServiceName.getOrElse("No Entity Service Set"))
    ).reduce(_ & _)

    val table = {
      currentEntities match {
        case ents if (ents.isEmpty) => "#entities" #> Text("No entities selected")
        case ents => ".entity *" #> ents.map(ent => {
          Seq(
            ".entity_id *" #> ent.entity.id,
            ".entity_types *" #> freebaseEntityTypeFilter(ent.entity.freebaseTypes).mkString(", "),
            ".entity_name *" #> ent.entity.name,
            ".entity_mentions *" #> ent.mentions.size,
            ".entity_clusters *" #> ent.clusters.mkString(", ")
          ).reduce(_ & _)
        })
      }
    }
    val nameSearch = {
      "#name_search" #> SHtml.ajaxText(nameQuery.getOrElse(""), t => {
        nameQuery = Some(t)
        entityQuery = Some(Query(EntityService.ByNameRegex(t), 0, 10))
        updateAll()
        reRender()
      })
    }
    val idSearch = {
      "#id_search" #> SHtml.ajaxText("", t => {
        entityQuery = Some(Query(EntityService.ById(t), 0, 10))
        updateAll()
        reRender()
      })
    }
    val clusterNames = {
      "#cluster_names" #> SHtml.ajaxText(this.clusterNames.getOrElse(""), t => {
        this.clusterNames = Some(t)
        import ServiceRegistry._
        val split = t.split(",").map(_.trim)
        val services = for (name <- split) yield {
          for (Service(_,service) <- Global.clusterServiceRegistry !! GetOrCreateService(name)) yield {
            ClusterServiceRef(name,service)
          }
        }
        val filtered = services.flatMap(_.toSeq)
        clusterServices = Some(filtered)
        updateAll()
        reRender()
      })
    }

    stats & table & nameSearch & idSearch & clusterNames

  }


}

object EntityListViewer {

  case class SetParams(entityServiceName: String, alignmentServiceName: Option[String] = None)


}
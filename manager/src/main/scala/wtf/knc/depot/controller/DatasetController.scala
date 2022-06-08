package wtf.knc.depot.controller

import java.util.UUID

import com.twitter.finagle.http.{Response, Status}
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.util.{Duration, Future}
import javax.inject.{Inject, Provider, Singleton}
import wtf.knc.depot.controller.DatasetController._
import wtf.knc.depot.dao._
import wtf.knc.depot.message.{Message, Publisher}
import wtf.knc.depot.model.Datatype.Table.ColumnType
import wtf.knc.depot.model._
import wtf.knc.depot.notebook.NotebookStore
import wtf.knc.depot.notebook.NotebookStore.NotebookContents
import wtf.knc.depot.service.{CloudService, TransitionHandler}

object DatasetController {
  trait DatasetRoute extends EntityRoute { val datasetTag: String }
  trait SegmentRoute extends DatasetRoute { val version: Long }

  private case class DatasetRequest(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String
  ) extends DatasetRoute

  private case class DatasetVersionRequest(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    @RouteParam version: Long
  ) extends SegmentRoute

  private case class DatasetTrigger(
    entityName: String,
    datasetTag: String
  )

  private case class DatasetCreateRequest(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    description: String,
    content: NotebookContents,
    datatype: Datatype,
    visibility: Visibility,
    triggers: Seq[DatasetTrigger],
    isolated: Boolean,
    retention: Option[Duration],
    schedule: Option[Duration],
    clusterAffinity: Option[String]
  ) extends DatasetRoute

  private case class ManageResponse(
    owner: Boolean
  )

  private case class DatasetCreateResponse(
    tag: String
  )

  private case class StatsResponse(
    numSegments: Long,
    totalSize: Long
  )

  private case class ProvenanceResponse(
    entityName: String,
    datasetTag: String,
    datatype: Datatype,
    version: Long,
    valid: Boolean,
    from: Seq[ProvenanceResponse]
  )

  private case class LineageResponse(
    entityName: String,
    datasetTag: String,
    datatype: Datatype,
    valid: Boolean,
    from: Seq[LineageResponse]
  )

  private case class SparkLocation(
    path: Seq[String],
    schema: String,
    format: String
  )

  private case class LocateResponse(
    self: SparkLocation,
    inputs: Map[String, SparkLocation]
  )

  case class AddCollaborator(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    collaboratorName: String,
    role: Role
  ) extends DatasetRoute

  case class RemoveCollaborator(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    collaboratorName: String
  ) extends DatasetRoute

  case class Collaborator(entity: Entity, role: Role)
  case class Collaborators(members: Seq[Collaborator])

  private case class FailRequest(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    @RouteParam version: Long,
    cause: String,
    errorMessage: String
  ) extends SegmentRoute

  private case class CommitRequest(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    @RouteParam version: Long,
    path: String,
    rows: Long,
    sample: Seq[Seq[String]]
  ) extends SegmentRoute
}

@Singleton
class DatasetController @Inject() (
  override val authProvider: Provider[Option[Auth]],
  override val clusterDAO: ClusterDAO,
  override val entityDAO: EntityDAO,
  datasetDAO: DatasetDAO,
  segmentDAO: SegmentDAO,
  transformationDAO: TransformationDAO,
  notebookDAO: NotebookDAO,
  notebookStore: NotebookStore,
  graphDAO: GraphDAO,
  cloudService: CloudService,
  transitionHandler: TransitionHandler,
  publisher: Publisher
) extends Controller
  with Authentication {

  // Does entity `entityId` have role `role` against dataset `dataset`?
  private def authorizeDataset(
    requester: Option[Entity],
    role: Option[Role],
    owner: Entity,
    dataset: Dataset
  ): Future[Boolean] = {
    (requester, role, dataset.visibility) match {
      case (_, None, _) => Future.True
      case (_, Some(Role.Member), Visibility.Public) => Future.True
      case (Some(entity), _, _) if entity == owner => Future.True
      case (Some(entity), Some(role), _) =>
        val asOrgMember = owner match {
          case Entity.Organization(id, _, _) => isMember(id, entity.id, role)
          case _ => Future.False
        }
        val asCollaborator = datasetDAO.collaborators(dataset.id).flatMap { collaborators =>
          entityDAO
            .whereMember(entity.id)
            .map(_ :+ entity)
            .map { targets =>
              targets.exists { e =>
                if (role == Role.Member) {
                  collaborators.contains(e.id)
                } else {
                  collaborators.get(e.id).contains(role)
                }
              }
            }
        }
        Future
          .join(asOrgMember, asCollaborator)
          .map(Function.tupled(_ || _))
      case _ => Future.False
    }
  }

  private def withDataset[A <: DatasetRoute](
    role: Option[Role]
  )(fn: (A, Dataset) => Future[Response]): A => Future[Response] = withAuth[A] { case (req, auth) =>
    Future
      .join(
        entityDAO.byName(req.entityName),
        datasetDAO.byTag(req.datasetTag)
      )
      .flatMap {
        case (Some(owner), Some(dataset)) =>
          lazy val deny = Future.value(response.forbidden)
          lazy val grant = fn(req, dataset)
          if (auth.contains(Auth.Admin)) {
            grant
          } else {
            auth
              .fold[Future[Option[Entity]]](Future.None) {
                case Auth.Cluster(clusterId) =>
                  clusterDAO.byId(clusterId).flatMap { cluster =>
                    entityDAO.byId(cluster.ownerId)
                  }
                case Auth.User(userId) =>
                  entityDAO.byId(userId)
                case _ => Future.None
              }
              .flatMap { maybeEntity => authorizeDataset(maybeEntity, role, owner, dataset) }
              .flatMap { if (_) grant else deny }
          }
        case _ => Future.value(response.notFound)
      }
  }

  private def withSegment[A <: SegmentRoute](
    role: Option[Role]
  )(fn: (A, Dataset, Segment) => Future[Response]): A => Future[Response] = withDataset[A](role) { (req, dataset) =>
    segmentDAO.byVersion(dataset.id, req.version).flatMap {
      case Some(segment) => fn(req, dataset, segment)
      case _ => Future.value(response.notFound(s"Version ${req.version} does not exist"))
    }
  }

  private def convertColumnType(columnType: ColumnType): String = columnType match {
    case ColumnType.String => "STRING"
    case ColumnType.Double => "DOUBLE"
    case ColumnType.Float => "FLOAT"
    case ColumnType.Integer => "INT"
  }

  private def convertDatatype(datatype: Datatype): (String, String) = datatype match {
    case Datatype.Raw() => "path STRING, modificationTime TIMESTAMP, length LONG, content BINARY" -> "binaryFile"
    case Datatype.Table(columns) =>
      columns
        .map { col => s"${col.name} ${convertColumnType(col.columnType)}" }
        .mkString(", ") -> "parquet"
  }

  private def sparkLocation(maybeSegment: Option[Segment], dataset: Dataset): Future[SparkLocation] = {
    val (schema, format) = convertDatatype(dataset.datatype)
    val loadPaths = maybeSegment match {
      case Some(segment) if segment.state == SegmentState.Materialized =>
        segmentDAO.data(segment.id).map(data => Seq(data.path))
      case _ =>
        Future.value(Seq.empty)
    }
    loadPaths.map { paths =>
      SparkLocation(paths, schema, format)
    }
  }

  private def locate(maybeSegment: Option[Segment], dataset: Dataset): Future[LocateResponse] = {
    val self = sparkLocation(maybeSegment, dataset)
    val inputs = maybeSegment match {
      case Some(segment) =>
        segmentDAO
          .inputs(segment.id)
          .flatMap { inputs =>
            Future.collect(inputs.map { input =>
              for {
                segment <- segmentDAO.byId(input.sourceSegmentId)
                dataset <- datasetDAO.byId(segment.datasetId)
                location <- sparkLocation(Some(segment), dataset)
              } yield { input.binding -> location }
            })
          }
          .map { locations =>
            locations.groupMapReduce(_._1)(_._2) { case (l1, l2) =>
              SparkLocation(l1.path ++ l2.path, l1.schema, l1.format)
            }
          }
      case _ => Future.value(Map.empty[String, SparkLocation])
    }
    for {
      location <- self
      locations <- inputs
    } yield LocateResponse(location, locations)
  }

  prefix("/api/entity/:entity_name/datasets") {
    get("/?") {
      withEntity[EntityRequest](None) { (_, owner) =>
        datasetDAO.byOwner(owner.id).map(response.ok)
      }
    }

    prefix("/:dataset_tag") {
      post("/?") {
        withEntity[DatasetCreateRequest](Some(Role.Owner)) { (req, owner) =>
          // TODO: kc - validate dataset info
          val notebookId = UUID.randomUUID().toString.replace("-", "")
          val inputMode = if (req.isolated) InputMode.Ancilla else InputMode.Trigger

          val saveNotebook = notebookDAO.create(notebookId, Entity.Root.id).before {
            notebookStore.save(notebookId, req.content)
          }

          // TODO: kc - can user access these datasets?
          // TODO: kc add entity info here
          val collectTriggers = Future
            .collect(req.triggers.map(t => datasetDAO.byTag(t.datasetTag)))
            .map { maybeDatasets =>
              require(maybeDatasets.forall(_.isDefined), "Invalid dataset tags")
              val datasets = maybeDatasets.flatten
              datasets.map(d => d.tag -> (d.id, inputMode)).toMap
            }
          val findCluster = req.clusterAffinity
            .map { c =>
              clusterDAO.byTag(c).map {
                case Some(cluster) if cluster.ownerId == owner.id => Some(cluster.id)
                case _ => throw new Exception(s"Cluster $c not found in $owner")
              }
            }
            .getOrElse(Future.None)

          val notifyCloud = cloudService.createDataset(owner.name, req.datasetTag)

          Future
            .join(notifyCloud, findCluster)
            .flatMap { case (_, clusterAffinity) =>
              datasetDAO
                .create(
                  req.datasetTag,
                  req.description,
                  owner.id,
                  req.datatype,
                  req.visibility,
                  req.retention,
                  req.schedule,
                  clusterAffinity
                )
                .flatMap { targetDatasetId =>
                  Future
                    .join(saveNotebook, collectTriggers)
                    .flatMap { case (_, inputs) =>
                      val createTransformation =
                        transformationDAO.create(targetDatasetId, notebookId).map(_ => targetDatasetId)
                      val createEdges = Future.join(inputs.toSeq.map { case (name, (datasetId, inputMode)) =>
                        graphDAO.make(targetDatasetId, datasetId, name, inputMode, valid = true)
                      })
                      val createSchedule = req.schedule match {
                        case Some(duration) => publisher.publish(Message.DatasetSchedule(targetDatasetId), duration)
                        case _ => Future.Done
                      }
                      val createAcl = datasetDAO.addCollaborator(targetDatasetId, owner.id, Role.Owner)
                      Future
                        .join(createTransformation, createEdges, createSchedule, createAcl)
                        .map(_ => targetDatasetId)
                    }
                    .flatMap { _ =>
                      transitionHandler.createSegment(targetDatasetId, Trigger.Creation(targetDatasetId))
                    }
                }
                .map { _ => response.ok }
            }
        }
      }

      get("/manage") { req: DatasetRequest =>
        val asAdmin = withDataset[DatasetRequest](Some(Role.Owner)) { (_, dataset) =>
          Future.value(response.ok(dataset))
        }(req)
        asAdmin.map { proxy =>
          if (proxy.status == Status.Ok) {
            response.ok(ManageResponse(true))
          } else {
            response.ok(ManageResponse(false))
          }
        }
      }

      get("/?") {
        withDataset[DatasetRequest](Some(Role.Member)) { (_, dataset) =>
          Future.value(response.ok(dataset))
        }
      }

      get("/locate") {
        withDataset[DatasetRequest](Some(Role.Member)) { (_, dataset) =>
          segmentDAO.list(dataset.id).flatMap { segments =>
            val materialized = segments.filter(_.state == SegmentState.Materialized)
            locate(materialized.maxByOption(_.version), dataset).map(response.ok)
          }
        }
      }

      get("/notebook") {
        withDataset[DatasetRequest](Some(Role.Member)) { (_, dataset) =>
          transformationDAO.get(dataset.id).flatMap {
            case Some(notebookId) => notebookStore.get(notebookId).map(response.ok)
            case _ => Future.value(response.notFound)
          }
        }
      }

      get("/stats") {
        withDataset[DatasetRequest](Some(Role.Member)) { (_, dataset) =>
          Future
            .join(
              segmentDAO.count(dataset.id),
              segmentDAO.size(dataset.id)
            )
            .map { case (count, size) => response.ok(StatsResponse(count, size)) }
        }
      }

      get("/sample") {
        withDataset[DatasetRequest](Some(Role.Member)) { (_, dataset) =>
          segmentDAO.list(dataset.id).flatMap { segments =>
            val lastMaterialized = segments
              .filter(_.state == SegmentState.Materialized)
              .sortBy(_.version)
              .lastOption
            lastMaterialized match {
              case Some(segment) => segmentDAO.data(segment.id).map(_.sample).map(response.ok)
              case _ => Future.value(response.ok(Seq.empty))
            }
          }
        }
      }

      get("/lineage") {
        def parents(dataset: Dataset): Future[LineageResponse] = {
          graphDAO.in(dataset.id).flatMap { edges =>
            val upstreams = edges.map { edge =>
              if (edge.valid) {
                datasetDAO
                  .byId(edge.sourceDatasetId)
                  .flatMap(parents)
              } else {
                for {
                  parent <- datasetDAO.byId(edge.sourceDatasetId)
                  owner <- entityDAO.byId(parent.ownerId).map(_.get)
                } yield LineageResponse(
                  owner.name,
                  parent.tag,
                  parent.datatype,
                  valid = false,
                  Seq.empty
                )
              }
            }
            entityDAO.byId(dataset.ownerId).flatMap {
              case Some(entity) =>
                Future.collect(upstreams).map { lineage =>
                  LineageResponse(entity.name, dataset.tag, dataset.datatype, valid = true, lineage)
                }
              case _ => throw new Exception(s"No owner for dataset ${dataset.id}")
            }
          }
        }

        withDataset[DatasetRequest](Some(Role.Member)) { (_, dataset) =>
          parents(dataset).map(response.ok)
        }
      }

      prefix("/collaborators") {
        get("/?") {
          withDataset[DatasetRequest](Some(Role.Owner)) { (_, dataset) =>
            datasetDAO.collaborators(dataset.id).flatMap { collabs =>
              val loadCollabs = collabs.map { case (id, role) =>
                entityDAO.byId(id).map {
                  case Some(entity) => Collaborator(entity, role)
                  case _ => throw new Exception(s"Unable to load entity $id")
                }
              }
              Future.collect(loadCollabs.toSeq).map { collabs =>
                response.ok(collabs)
              }
            }
          }
        }

        delete("/?") {
          withDataset[RemoveCollaborator](Some(Role.Owner)) { (req, dataset) =>
            entityDAO.byName(req.collaboratorName).flatMap {
              case Some(entity) =>
                if (entity.id == dataset.ownerId) {
                  Future.value(response.badRequest(s"Cannot remove dataset owner from ACL"))
                } else {
                  cloudService.delDatasetACL(entity.name, req.entityName, dataset.tag).before {
                    datasetDAO.removeCollaborator(dataset.id, entity.id).map(_ => response.ok)
                  }
                }
              case _ => Future.value(response.notFound)
            }
          }
        }

        post("/?") {
          withDataset[AddCollaborator](Some(Role.Owner)) { (req, dataset) =>
            entityDAO.byName(req.collaboratorName).flatMap {
              case Some(entity) =>
                cloudService.addDatasetACL(entity.name, req.entityName, req.datasetTag).before {
                  datasetDAO.addCollaborator(dataset.id, entity.id, req.role).map(_ => response.ok)
                }
              case _ => Future.value(response.notFound)
            }
          }
        }
      }

      prefix("/segments") {
        get("/?") {
          withDataset[DatasetRequest](Some(Role.Member)) { (_, dataset) =>
            segmentDAO.list(dataset.id).map(response.ok)
          }
        }

        prefix("/:version") {
          get("/?") {
            withSegment[DatasetVersionRequest](Some(Role.Member)) { (_, _, segment) =>
              Future.value(response.ok(segment))
            }
          }

          post("/commit") {
            withSegment[CommitRequest](Some(Role.Owner)) { (req, _, segment) =>
              cloudService.size(req.path).flatMap { size =>
                val data = SegmentData(req.path, "", size, req.rows, req.sample)
                publisher
                  .publish(Message.SegmentTransition(segment.id, Transition.Materialize(data)))
                  .map(_ => response.created)
              }
            }
          }

          post("/fail") {
            withSegment[FailRequest](Some(Role.Owner)) { (req, _, segment) =>
              publisher
                .publish(Message.SegmentTransition(segment.id, Transition.Fail(req.cause, req.errorMessage)))
                .map(_ => response.created)
            }
          }

          post("/materialize") {
            withSegment[DatasetVersionRequest](Some(Role.Owner)) { (_, _, segment) =>
              if (segment.state == SegmentState.Announced) {
                publisher
                  .publish(Message.SegmentTransition(segment.id, Transition.Await(Trigger.Manual(-1))))
                  .map(_ => response.created(segment.id))
              } else {
                Future.value(response.badRequest(s"Segment has state ${segment.state}"))
              }
            }
          }

          get("/history") {
            withSegment[DatasetVersionRequest](Some(Role.Member)) { (_, _, segment) =>
              segmentDAO.transitions(segment.id).map(response.ok)
            }
          }

          get("/locate") {
            withSegment[DatasetVersionRequest](Some(Role.Member)) { (_, dataset, segment) =>
              locate(Some(segment), dataset).map(response.ok)
            }
          }

          get("/provenance") {
            def parents(segment: Segment): Future[ProvenanceResponse] = {
              datasetDAO.byId(segment.datasetId).flatMap { dataset =>
                val upstreams = segmentDAO.inputs(segment.id).flatMap { inputs =>
                  Future.collect(inputs.map { input =>
                    segmentDAO.byId(input.sourceSegmentId).flatMap(parents)
                  })
                }

                entityDAO.byId(dataset.ownerId).flatMap {
                  case Some(entity) =>
                    upstreams.map { provenance =>
                      ProvenanceResponse(
                        entity.name,
                        dataset.tag,
                        dataset.datatype,
                        segment.version,
                        valid = true,
                        provenance
                      )
                    }
                  case _ => throw new Exception(s"No owner for dataset ${dataset.id}")
                }
              }
            }

            withSegment[DatasetVersionRequest](Some(Role.Member)) { (_, _, segment) =>
              parents(segment).map(response.ok)
            }
          }
        }
      }
    }
  }
}

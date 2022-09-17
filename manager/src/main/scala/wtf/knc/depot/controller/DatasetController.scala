package wtf.knc.depot.controller

import java.net.URLEncoder
import java.nio.charset.Charset
import java.util.{Base64, UUID}

import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.inject.annotations.Flag
import com.twitter.util.{Duration, Future, FuturePool}
import javax.inject.{Inject, Provider, Singleton}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.S3Object
import wtf.knc.depot.controller.DatasetController._
import wtf.knc.depot.dao._
import wtf.knc.depot.message.{Message, Publisher}
import wtf.knc.depot.model.Datatype.Table.ColumnType
import wtf.knc.depot.model._
import wtf.knc.depot.notebook.NotebookStore
import wtf.knc.depot.notebook.NotebookStore.NotebookContents
import wtf.knc.depot.service.{CloudService, TransitionHandler}

import scala.util.control.NonFatal

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

  private case class CreateDataset(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    description: String,
    origin: Origin,
    content: NotebookContents,
    datatype: Datatype,
    visibility: Visibility,
    storageClass: StorageClass,
    triggers: Seq[DatasetTrigger],
    isolated: Boolean,
    retention: Option[Duration],
    schedule: Option[Duration],
    clusterAffinity: Option[String]
  ) extends DatasetRoute

  private case class UploadDataset(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    files: Map[String, String]
  ) extends DatasetRoute

  private case class UpdateDataset(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    description: String,
    retention: Option[Duration],
    schedule: Option[Duration],
    visibility: Visibility
  ) extends DatasetRoute

  private case class ManageResponse(
    owner: Boolean
  )

  private case class SampleResponse(
    sample: Seq[Seq[String]],
    rows: Long
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
  @Flag("deployment.id") deployment: String,
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
  publisher: Publisher,
  s3: RestS3Service
) extends Controller
  with EntityRequests
  with Authentication {
  private final val UploadBucket = s"$deployment.uploads"

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

  private def dataset(role: Option[Role])(implicit req: DatasetRoute): Future[Dataset] = entity(None)
    .flatMap { owner =>
      datasetDAO.byTag(req.datasetTag).flatMap {
        case Some(dataset) =>
          lazy val deny = response.forbidden.toFutureException
          lazy val grant = Future.value(dataset)
          if (_client.contains(Auth.Admin)) {
            grant
          } else {
            _client
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

        case _ => throw response.notFound.toException
      }
    }

  private def segment(role: Option[Role])(implicit req: SegmentRoute): Future[Segment] = dataset(role)
    .flatMap { dataset =>
      segmentDAO.byVersion(dataset.id, req.version).map {
        case Some(segment) => segment
        case _ => throw response.notFound.toException
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
        segmentDAO.data(segment.id).map {
          case Some(data) => Seq(data.path)
          case _ => Seq.empty
        }
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
    get("/?") { implicit req: EntityRequest =>
      entity(None).flatMap { owner =>
        val authFilter: Dataset => Future[Boolean] = _client match {
          case Some(Auth.User(userId)) =>
            dataset =>
              entityDAO.byId(userId).flatMap { maybeEntity =>
                authorizeDataset(maybeEntity, Some(Role.Member), owner, dataset)
              }
          case Some(Auth.Cluster(clusterId)) =>
            dataset =>
              clusterDAO.byId(clusterId).flatMap { cluster =>
                entityDAO.byId(cluster.ownerId).flatMap { maybeEntity =>
                  authorizeDataset(maybeEntity, Some(Role.Member), owner, dataset)
                }
              }
          case Some(Auth.Admin) => _ => Future.True
          case _ => authorizeDataset(None, Some(Role.Member), owner, _)
        }

        datasetDAO.byOwner(owner.id).flatMap { datasets =>
          val filtered = datasets.map { dataset =>
            authFilter(dataset).map { if (_) Some(dataset) else None }
          }
          Future.collect(filtered).map(_.flatten)
        }
      }
    }

    prefix("/:dataset_tag") {
      get("/?") { implicit req: DatasetRequest =>
        dataset(Some(Role.Member)).map(response.ok)
      }

      post("/?") { implicit req: CreateDataset =>
        entity(Some(Role.Owner)).flatMap { owner =>
          if (req.origin == Origin.Unmanaged) {
            require(req.retention.isEmpty)
            require(req.clusterAffinity.isEmpty)
            require(req.schedule.isEmpty)
            require(req.triggers.isEmpty)
            require(req.content.isEmpty)
            // no notebook
            // no message dispatch
          }

          // TODO: kc - validate dataset info
          val inputMode = if (req.isolated) InputMode.Ancilla else InputMode.Trigger

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
                  req.origin,
                  req.datatype,
                  req.visibility,
                  req.storageClass,
                  req.retention,
                  req.schedule,
                  clusterAffinity
                )
                .flatMap { targetDatasetId =>
                  collectTriggers
                    .flatMap { inputs =>
                      val createTransformation = if (req.origin == Origin.Managed) {
                        val notebookId = UUID.randomUUID().toString.replace("-", "")
                        notebookDAO
                          .create(notebookId, Entity.Root.id)
                          .before { notebookStore.save(notebookId, req.content) }
                          .before { transformationDAO.create(targetDatasetId, notebookId).map(_ => targetDatasetId) }
                      } else {
                        Future.Done
                      }

                      val createEdges = Future.join(inputs.toSeq.map { case (name, (datasetId, inputMode)) =>
                        graphDAO.make(targetDatasetId, datasetId, name, inputMode, valid = true)
                      })
                      val dispatchMessages = datasetDAO.byId(targetDatasetId).flatMap { dataset =>
                        val createSchedule = req.schedule match {
                          case Some(duration) =>
                            publisher.publish(Message.DatasetSchedule(targetDatasetId, dataset.updatedAt), duration)
                          case _ => Future.Done
                        }
                        val createPruner = req.retention match {
                          case Some(retention) =>
                            publisher.publish(Message.DatasetPrune(targetDatasetId, dataset.updatedAt))
                          case _ => Future.Done
                        }
                        Future.join(createSchedule, createPruner)
                      }
                      val createAcl = datasetDAO.addCollaborator(targetDatasetId, owner.id, Role.Owner)
                      Future
                        .join(createTransformation, createEdges, dispatchMessages, createAcl)
                        .map(_ => targetDatasetId)
                    }
                    .flatMap { _ =>
                      if (req.origin == Origin.Managed) {
                        transitionHandler.createSegment(targetDatasetId, Trigger.Creation(targetDatasetId))
                      } else {
                        Future.Done
                      }
                    }
                }
                .map { _ => response.created }
            }
        }
      }

      post("/upload") { implicit req: UploadDataset =>
        entity(Some(Role.Member)).flatMap { owner =>
          dataset(Some(Role.Owner)).flatMap { dataset =>
            require(dataset.origin == Origin.Unmanaged, "Only unmanaged datasets can accept file uploads")
            require(req.files.nonEmpty, "Dataset cannot be empty")

            logger.info(s"Creating new segment for ${owner.name}/${dataset.tag} with ${req.files}")
            val id = _client match {
              case Some(Auth.User(userId)) => userId
              case _ => -1
            }
            segmentDAO.make(dataset.id).flatMap { segmentId =>
              segmentDAO
                .byId(segmentId)
                .flatMap { segment =>
                  transitionHandler.initializeSegment(dataset, segment).map { _ => segment }
                }
                .flatMap { segment =>
                  val (bucket, root) = cloudService.allocatePath(owner, dataset, segment)
                  val copyFiles = req.files.toSeq.map { case (key, fileId) =>
                    val filename = URLEncoder.encode(key, Charset.defaultCharset())
                    val uploadPath = s"${owner.name}/$fileId"
                    val targetPath = s"$root/$filename"
                    FuturePool.unboundedPool {
                      logger.info(s"Copying file $uploadPath to $targetPath")
                      s3.copyObject(
                        UploadBucket,
                        uploadPath,
                        bucket,
                        new S3Object(targetPath),
                        false
                      )

                      val meta = s3.getObjectDetails(UploadBucket, uploadPath)
                      val head = s3.getObject(UploadBucket, uploadPath, null, null, null, null, 0L, 999L)
                      try {
                        val sample = Base64.getEncoder.encode(head.getDataInputStream.readNBytes(1000))
                        Seq(filename, meta.getContentLength, meta.getMetadata("Depot-Content-Type"), new String(sample))
                      } finally {
                        head.getDataInputStream.close()
                      }
                    }
                  }
                  Future
                    .collect(copyFiles)
                    .flatMap { fileData =>
                      val data = SegmentData(
                        s"s3a://$bucket/$root",
                        "",
                        fileData.map(_(1).toString.toInt).sum,
                        0,
                        fileData.map(_.map(_.toString))
                      )
                      publisher
                        .publish(
                          Message
                            .SegmentTransition(segmentId, Transition.Materialize(data, Trigger.Manual(id, owner.id)))
                        )
                    }
                }
                .rescue { case NonFatal(e) =>
                  logger.error(s"Failed to copy uploaded files", e)
                  segmentDAO.delete(segmentId).before(Future.exception(e))
                }
            }
          }
        }
      }

      patch("/?") { implicit req: UpdateDataset =>
        dataset(Some(Role.Owner)).flatMap { dataset =>
          logger.info(s"Handling dataset update for ${dataset.tag}: $req")
          datasetDAO
            .update(dataset.id, req.description, req.retention, req.schedule, req.visibility)
            .before {
              entityDAO.byId(dataset.ownerId).flatMap {
                case Some(owner) =>
                  graphDAO.out(dataset.id).flatMap { edges =>
                    val downstreams = edges.map { edge =>
                      logger.info(s"Checking auth status for edge $edge")

                      for {
                        target <- datasetDAO.byId(edge.targetDatasetId)
                        maybeEntity <- entityDAO.byId(target.ownerId)
                        authed <- {
                          logger.info(s"Checking target dataset ${target.tag}")
                          authorizeDataset(maybeEntity, Some(Role.Owner), owner, dataset)
                        }
                        _ <- {
                          logger.info(s"Target dataset ${target.tag} access to ${dataset.tag}: $authed")
                          graphDAO.updateEdge(target.id, dataset.id, edge.binding, authed)
                        }
                      } yield ()
                    }
                    Future.join(downstreams)
                  }
                case _ => throw new Exception(s"Dataset ${dataset.tag} has no owner")
              }
            }
            .before {
              datasetDAO.byId(dataset.id).flatMap { dataset =>
                if (dataset.origin == Origin.Managed) {
                  req.schedule match {
                    case Some(duration) =>
                      publisher.publish(Message.DatasetSchedule(dataset.id, dataset.updatedAt), duration)
                    case _ => Future.Done
                  }
                } else {
                  Future.Done
                }
              }
            }
            .map(_ => response.noContent)
        }
      }

      get("/manage") { implicit req: DatasetRequest =>
        dataset(Some(Role.Owner))
          .map { _ => true }
          .handle { _ => false }
          .map { a => response.ok(ManageResponse(a)) }
      }

      get("/locate") { implicit req: DatasetRequest =>
        dataset(Some(Role.Member)).flatMap { dataset =>
          segmentDAO.list(dataset.id).flatMap { segments =>
            val materialized = segments.filter(_.state == SegmentState.Materialized)
            locate(materialized.maxByOption(_.version), dataset).map(response.ok)
          }
        }
      }

      get("/notebook") { implicit req: DatasetRequest =>
        dataset(Some(Role.Member)).flatMap { dataset =>
          transformationDAO.get(dataset.id).flatMap {
            case Some(notebookId) => notebookStore.get(notebookId).map(response.ok)
            case _ => Future.value(response.notFound)
          }
        }
      }

      get("/stats") { implicit req: DatasetRequest =>
        dataset(Some(Role.Member)).flatMap { dataset =>
          Future
            .join(
              segmentDAO.count(dataset.id),
              segmentDAO.size(dataset.id)
            )
            .map { case (count, size) => response.ok(StatsResponse(count, size)) }
        }
      }

      get("/sample") { implicit req: DatasetRequest =>
        dataset(Some(Role.Member)).flatMap { dataset =>
          segmentDAO.list(dataset.id).flatMap { segments =>
            val lastMaterialized = segments
              .filter(_.state == SegmentState.Materialized)
              .sortBy(_.version)
              .lastOption
            lastMaterialized match {
              case Some(segment) =>
                segmentDAO.data(segment.id).map {
                  case Some(data) => response.ok(SampleResponse(data.sample, data.rows))
                  case _ => response.notFound
                }
              case _ => Future.value(response.ok(Seq.empty))
            }
          }
        }
      }

      get("/lineage") { implicit req: DatasetRequest =>
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

        dataset(Some(Role.Member)).flatMap { dataset =>
          parents(dataset).map(response.ok)
        }
      }

      prefix("/collaborators") {
        get("/?") { implicit req: DatasetRequest =>
          dataset(Some(Role.Owner)).flatMap { dataset =>
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

        delete("/?") { implicit req: RemoveCollaborator =>
          dataset(Some(Role.Owner)).flatMap { dataset =>
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

        post("/?") { implicit req: AddCollaborator =>
          dataset(Some(Role.Owner)).flatMap { dataset =>
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
        get("/?") { implicit req: DatasetRequest =>
          dataset(Some(Role.Member)).flatMap { dataset =>
            segmentDAO.list(dataset.id).map(response.ok)
          }
        }

        prefix("/:version") {
          get("/?") { implicit req: DatasetVersionRequest =>
            segment(Some(Role.Member)).map(response.ok)
          }

          get("/download") { implicit req: DatasetVersionRequest =>
            segment(Some(Role.Member)).flatMap { segment =>
              require(segment.state == SegmentState.Materialized)
              segmentDAO.data(segment.id).flatMap {
                case Some(data) =>
                  cloudService
                    .downloadPath(data.path)
                    .map(response.ok)

                case _ => throw response.notFound.toException
              }
            }
          }

          post("/commit") { implicit req: CommitRequest =>
            segment(Some(Role.Owner)).flatMap { segment =>
              cloudService.size(req.path).flatMap { size =>
                val data = SegmentData(req.path, "", size, req.rows, req.sample)
                val trigger = Trigger.System()
                publisher
                  .publish(Message.SegmentTransition(segment.id, Transition.Materialize(data, trigger)))
                  .map(_ => response.created)
              }
            }
          }

          post("/fail") { implicit req: FailRequest =>
            segment(Some(Role.Owner)).flatMap { segment =>
              publisher
                .publish(Message.SegmentTransition(segment.id, Transition.Fail(req.cause, req.errorMessage)))
                .map(_ => response.created)
            }
          }

          post("/materialize") { implicit req: DatasetVersionRequest =>
            segment(Some(Role.Owner)).flatMap { segment =>
              if (segment.state == SegmentState.Announced) {
                val id = client match {
                  case Some(Auth.User(userId)) => userId
                  case _ => -1
                }
                // TODO: Allow all authorized users to materialize datasets and select a quota (via org) to consume
                publisher
                  .publish(Message.SegmentTransition(segment.id, Transition.Await(Trigger.Manual(id, id))))
                  .map(_ => response.created(segment.id))
              } else {
                Future.value(response.badRequest(s"Segment has state ${segment.state}"))
              }
            }
          }

          get("/history") { implicit req: DatasetVersionRequest =>
            segment(Some(Role.Member)).flatMap { segment =>
              segmentDAO.transitions(segment.id).map(response.ok)
            }
          }

          get("/locate") { implicit req: DatasetVersionRequest =>
            dataset(Some(Role.Member)).flatMap { dataset =>
              segment(Some(Role.Member)).flatMap { segment =>
                locate(Some(segment), dataset).map(response.ok)
              }
            }
          }

          get("/provenance") { implicit req: DatasetVersionRequest =>
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

            segment(Some(Role.Member)).flatMap { segment =>
              parents(segment).map(response.ok)
            }
          }
        }
      }
    }
  }
}

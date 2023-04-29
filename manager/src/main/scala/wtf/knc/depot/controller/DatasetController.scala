package wtf.knc.depot.controller

import java.net.URLEncoder
import java.nio.charset.Charset
import java.util.{Base64, UUID}
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Http
import com.twitter.finagle.http.{MediaType, Method, Request, Version}
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.inject.annotations.Flag
import com.twitter.util.jackson.ScalaObjectMapper
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
import scala.util.parsing.json.JSONObject

object DatasetController {
  trait DatasetRoute extends EntityRoute { val datasetTag: String }
  trait DatasetByIdRoute extends EntityRoute {val datasetId: Long}
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

  private case class DatasetTopicRequest(
     @RouteParam topic: String
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
    clusterAffinity: Option[String],
    topic: String,
    window: Long,
    bootstrapServer:String
  ) extends DatasetRoute

  private case class UploadDataset(
    @RouteParam entityName: String,
    @RouteParam datasetTag: String,
    files: Map[String, String]
  ) extends DatasetRoute

  private case class CreateTopicSegment(
    @RouteParam entityName: String,
    @RouteParam datasetId: Long,
    topic: String,
    partition: Long,
    start_offset: Long,
    end_offset: Long,
    notebook_tag: String,
    bootstrap_server: String
  ) extends DatasetByIdRoute

  private case class AllocateSegmentRequest(
    @RouteParam datasetId: Long,
    @RouteParam segmentId: Long,
    @RouteParam entityName: String,
                                           ) extends EntityRoute

  private case class AllocatePathResponse(
                                               bucket: String,
                                               datasetTag: String,
                                               root: String )


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
    numSegments: Long
  )

  private case class StreamingSegmentResponse(
    datasetTag: String,
    segmentId: Long,
    segmentVersion: Long)


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

  case class ConsumerRequest(
    notebookTag: String,
                              topic: String,
                              datasetTag: String,
                              datasetId: Long,
                              window: Long,
                              bootstrapServer: String)

  case class ConsumerResponse(
                               success: String
                             )


  case class AnnounceStreamingRequest(
                                       datasetId: Long,
                                       segmentId: Long,
                                       segmentVersion: Long,
                                       startOffset: Long,
                                       endOffset: Long,
                                       notebookTag: String,
                                       topic: String,
                                       bootstrapServer: String
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
  override val datasetDAO: DatasetDAO,
  override val segmentDAO: SegmentDAO,
  transformationDAO: TransformationDAO,
  streamingTransformationDAO: StreamingTransformationDAO,
  notebookDAO: NotebookDAO,
  notebookStore: NotebookStore,
  graphDAO: GraphDAO,
  cloudService: CloudService,
  transitionHandler: TransitionHandler,
  publisher: Publisher,
  s3: RestS3Service,
  objectMapper: ScalaObjectMapper,
) extends Controller
  with EntityRequests
  with DatasetRequests
  with Authentication {
  private final val UploadBucket = s"$deployment.uploads"

  private def createResponseForStreamingSegment(datasetTag: String, segmentId: Long, segmentVersion:Long): Future[StreamingSegmentResponse] = {
      return Future.value((StreamingSegmentResponse(datasetTag, segmentId, segmentVersion)))
  }

  private def createAllocatePathResponse(bucket: String, datasetTag: String, root: String): AllocatePathResponse = {
    return AllocatePathResponse(bucket, datasetTag, root)
  }

  private def convertColumnType(columnType: ColumnType): String = columnType match {
    case ColumnType.String => "STRING"
    case ColumnType.Double => "DOUBLE"
    case ColumnType.Float => "FLOAT"
    case ColumnType.Integer => "INT"
    case ColumnType.Long => "LONG"
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

  private def start_consumer(
                      notebookTag: String,
                      datasetId: Long,
                      datasetTag: String,
                      topic: String,
                      window: Long,
                      bootstrapServer: String
                    ): Future[String] = {

    datasetDAO.byId(datasetId).flatMap { dataset =>
      clusterDAO.byOwner(dataset.ownerId).flatMap { clusterList =>
        clusterDAO.consumer(clusterList.headOption.get.id).flatMap {
          case (Some(consumerInfo)) =>
            val client = Http.client.newService(consumerInfo.consumer)
            val body = ConsumerRequest(notebookTag, topic, datasetTag, datasetId, window, bootstrapServer)
            val data = objectMapper.writeValueAsBuf(body)
            val request = Request(Version.Http11, Method.Post, "/consume")
            request.content = data
            request.contentType = MediaType.Json
            client(request)
              .map { response =>
                objectMapper.parse[ConsumerResponse](response.content)
              }.map(_.success)
              .ensure(client.close())
          case _ => Future.exception(new Exception(s"Cluster ${clusterList.headOption.get.id} does not have a consumer"))
        }
      }
    }
  }

  private def start_stream_announce(
                              dataset_id: Long,
                              segment_id: Long,
                              segment_version: Long,
                              start_offset: Long,
                              end_offset: Long,
                              topic: String,
                              notebook_tag: String,
                              bootstrap_server: String
                            ): Future[String] = {
    datasetDAO.byId(dataset_id).flatMap { dataset =>
      clusterDAO.byOwner(dataset.ownerId).flatMap { clusterList =>
        clusterDAO.consumer(clusterList.headOption.get.id).flatMap {
            case (Some(consumerInfo)) =>
              val client = Http.client.newService(consumerInfo.consumer)
              val body = AnnounceStreamingRequest(dataset_id, segment_id, segment_version, start_offset, end_offset, notebook_tag, topic, bootstrap_server)
              val data = objectMapper.writeValueAsBuf(body)
              val request = Request(Version.Http11, Method.Post, "/announce")
              request.content = data
              request.contentType = MediaType.Json
              client(request)
                .map { response =>
                  objectMapper.parse[ConsumerResponse](response.content)
                }.map(_.success)
                .ensure(client.close())
            case _ => Future.exception(new Exception(s"Cluster ${clusterList.headOption.get.id} does not have a consumer"))
          }
        }
    }
  }

  prefix("/api/entity/:entity_name/datasets") {
    get("/?") { implicit req: EntityRequest =>
      entity(None).flatMap { owner =>
        val authFilter: Dataset => Future[Boolean] = client match {
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
            authFilter(dataset).map {
              if (_) Some(dataset) else None
            }
          }
          Future.collect(filtered).map(_.flatten)
        }
      }
    }


    prefix("/topics/:topic") {
      get("/?") { implicit req: DatasetTopicRequest =>
        streamingTransformationDAO.getByTopic(req.topic).map(response.ok)
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
            require(req.topic.isEmpty)
            require(req.window.isNaN)
            require(req.bootstrapServer.isEmpty)
            // no notebook
            // no message dispatch
          }
          if (req.origin == Origin.Managed) {
            require(req.topic.isEmpty)
            require(req.window.isNaN)
            require(req.bootstrapServer.isEmpty)
          }

          require(req.retention.forall(_ >= 1.minute))
          require(req.schedule.forall(_ >= 1.minute))
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
                          .before {
                            notebookStore.save(notebookId, req.content)
                          }
                          .before {
                            transformationDAO.create(targetDatasetId, notebookId).map(_ => targetDatasetId)
                          }
                      }
                      else if (req.origin == Origin.Streaming) {
                        val notebookId = UUID.randomUUID().toString.replace("-", "")
                        notebookDAO
                          .create(notebookId, Entity.Root.id)
                          .before {
                            notebookStore.save(notebookId, req.content)
                          }
                          .before {
                            transformationDAO.create(targetDatasetId, notebookId)
                          }
                          .before {
                            streamingTransformationDAO.create(targetDatasetId, req.topic, notebookId).map(_ => targetDatasetId)
                            start_consumer(notebookId, targetDatasetId, req.datasetTag,  req.topic, req.window, req.bootstrapServer)
                          }

                        }
                      else {
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
            val id = client match {
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
                      val size = fileData.map(_(1).toString.toInt).sum
                      val data = SegmentData(
                        s"s3a://$bucket/$root",
                        "",
                        size,
                        0,
                        fileData.map(_.map(_.toString))
                      )
                      publisher
                        .publish(
                          Message
                            .SegmentTransition(
                              segmentId,
                              Transition.Materialize(data, Trigger.Manual(id, owner.id), size, size)
                            )
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
          require(req.retention.forall(_ >= 1.minute))
          require(req.schedule.forall(_ >= 1.minute))
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
          segmentDAO.count(dataset.id).map { count => response.ok(StatsResponse(count)) }
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
              case _ => Future.value(response.ok(SampleResponse(Seq.empty, 0)))
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
                  LineageResponse(entity.name, dataset.tag, dataset.datatype, valid = true, lineage.sortBy(_.datasetTag))
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

                def retainedSize(id: Long): Future[Long] = {
                  val parentSize = segmentDAO
                    .inputs(id)
                    .map(_.map(_.sourceSegmentId))
                    .flatMap(s => Future.collect(s.map(retainedSize)))
                    .map(_.sum)
                  val shallowSize = segmentDAO.data(id).map(_.fold(0L)(_.size))
                  Future.join(parentSize, shallowSize).map(Function.tupled(_ + _))
                }

                retainedSize(segment.id).flatMap { retained =>
                  val tr = Transition.Materialize(data, trigger, size, retained + size)
                  val msg = Message.SegmentTransition(segment.id, tr)
                  publisher
                    .publish(msg)
                    .map(_ => response.created)
                }
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

          prefix("/materialize") {
            post("/?") { implicit req: DatasetVersionRequest =>
              segment(Some(Role.Owner)).flatMap { segment =>
                if (segment.state == SegmentState.Announced) {
                  val id = client match {
                    case Some(Auth.User(userId)) => userId
                    case _ => -1
                  }
                  publisher
                    .publish(Message.SegmentTransition(segment.id, Transition.Await(Trigger.Manual(id, id))))
                    .map(_ => response.created(segment.id))
                } else {
                  Future.value(response.badRequest(s"Segment has state ${segment.state}"))
                }
              }
            }
            prefix("/stream") {
              post("/?") { implicit req: DatasetVersionRequest =>
                val announced = dataset(Some(Role.Owner)).flatMap { dataset =>
                  segmentDAO.getSegmentAnnounce(dataset.id, req.version)
                }.flatMap { segment =>
                  start_stream_announce(segment.datasetId,  segment.segmentId, segment.segmentVersion, segment.startOffset, segment.endOffset, segment.topic, segment.notebookTag, segment.bootstrapServer)
                }.map{ response =>
                    response.contains("true")
                }
               announced.flatMap {
                 case true =>
                  Future.value(response.created("segment created successfully"))
                 case false =>
                 Future.value(response.internalServerError)
                }
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
    prefix("/:dataset_id/segment") {
      post("/?") { implicit req: CreateTopicSegment =>
        entity(Some(Role.Member)).flatMap { owner =>
          datasetDAO.byId(req.datasetId).flatMap { dataset =>
            require(dataset.origin == Origin.Streaming, "Only streaming dataset can call this endpoint")
            logger.info(s"Creating new segment for ${owner.name}/${dataset.tag}")
            val id = client match {
              case Some(Auth.User(userId)) => userId
              case _ => -1
            }
            segmentDAO.make(dataset.id).flatMap { segmentId =>
              segmentDAO
                .byId(segmentId).flatMap { segment => {
                segmentDAO.insertSegmentAnnounce(req.datasetId, segment.id, segment.version, req.topic, req.start_offset, req.end_offset, req.notebook_tag, req.bootstrap_server)
                createResponseForStreamingSegment(dataset.tag, segment.id, segment.version)
              }
              }.join {
                transitionHandler.handleTransition(segmentId, Transition.Announce.apply(Trigger.Creation(dataset.id)))
              }
            }
          }.map(_._1)
        }
      }
        post("/:segment_id") { implicit req: AllocateSegmentRequest =>
          entity(Some(Role.Member)).flatMap { owner =>
            datasetDAO.byId(req.datasetId).flatMap { dataset =>
              require(dataset.origin == Origin.Streaming, "Only streaming dataset can call this endpoint")
              logger.info(s"Creating new segment for ${owner.name}/${dataset.tag}")
              val id = client match {
                case Some(Auth.User(userId)) => userId
                case _ => -1
              }
              segmentDAO.byId(req.segmentId).map { segment =>
                val (bucket, root) = cloudService.allocatePath(owner, dataset, segment)
                createAllocatePathResponse(bucket, dataset.tag, root)
              }
            }
          }.map(response.ok)
        }
    }
  }
}

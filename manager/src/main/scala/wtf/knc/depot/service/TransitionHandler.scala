package wtf.knc.depot.service

import java.util.UUID

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.inject.Logging
import com.twitter.util.Future
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.dao.SegmentDAO.SegmentNotFoundException
import wtf.knc.depot.dao._
import wtf.knc.depot.message.{Message, Publisher}
import wtf.knc.depot.model.{InputMode, SegmentData, SegmentState, Transition, Trigger}
import wtf.knc.depot.notebook.TransformDispatcher

@Singleton
class TransitionHandler @Inject() (
  segmentDAO: SegmentDAO,
  publisher: Publisher,
  transformationDAO: TransformationDAO,
  graphDAO: GraphDAO,
  datasetDAO: DatasetDAO,
  entityDAO: EntityDAO,
  clusterDAO: ClusterDAO,
  cloudService: CloudService,
  transformClient: TransformDispatcher
) extends Logging {
  private case class InvalidTransitionException() extends Exception

  def createSegment(datasetId: Long, trigger: Trigger): Future[Unit] = {
    generateNewSegment(datasetId, trigger)(segmentDAO.defaultCtx)
  }

  def uploadSegment(datasetId: Long, data: SegmentData): Future[Unit] = {
    segmentDAO.make(datasetId)(segmentDAO.defaultCtx).flatMap { segmentId =>
      requestTransition(segmentId, Transition.Materialize(data))
    }
  }

  def handleTransition(segmentId: Long, transition: Transition): Future[Unit] = {
    logger.info(s"Handling transition request for $segmentId: $transition")
    segmentDAO
      .update(segmentId, transition.to) { implicit ctx =>
        { case (from, _) =>
          val work = (from, transition) match {
            case _ -> Transition.Fail(cause, error) =>
              logger.error(s"Segment $segmentId has failed: $cause - $error")
              propagateFailure(segmentId, error)

            case SegmentState.Initializing -> Transition.Materialize(data) =>
              logger.info(s"Propagating announcements for segment $segmentId")
              segmentDAO.setData(segmentId, data).before {
                propagateAnnouncement(segmentId)
              }

            case SegmentState.Initializing -> Transition.Announce(trigger) =>
              logger.info(s"Propagating announcements for segment $segmentId, triggered by: $trigger")
              propagateAnnouncement(segmentId)

            case SegmentState.Announced -> Transition.Await(trigger) =>
              logger.info(s"Materializing tree rooted at segment $segmentId, triggered by: $trigger")
              materializeTree(segmentId)

            case SegmentState.Awaiting -> Transition.Enqueue() =>
              logger.info(s"Dispatching transformation for segment $segmentId")
              requestTransition(segmentId, Transition.Transform())

            case SegmentState.Queued -> Transition.Transform() =>
              logger.info(s"Segment $segmentId has begun transforming")
              dispatchTransformation(segmentId)

            case SegmentState.Transforming -> Transition.Materialize(data) =>
              logger.info(s"Segment $segmentId was materialized with $data")
              logger.info(s"Activating waiting segments with segment $segmentId as input")
              segmentDAO.setData(segmentId, data).before {
                activateWaiters(segmentId)
              }

            case from -> to =>
              logger.error(s"Invalid transition $from -> $to")
              Future.exception(InvalidTransitionException())
          }
          work
            .before {
              segmentDAO.recordTransition(segmentId, transition)
            }
            .raiseWithin(10.seconds)(DefaultTimer)
        }
      }
      .onSuccess { _ => logger.info(s"Handled transition request for $segmentId: $transition") }
      .onFailure { ex => logger.error(s"Failed to handle transition request for $segmentId: $transition", ex) }
      .rescue { case _: InvalidTransitionException | _: SegmentNotFoundException =>
        Future.Done
      }
  }

  private def requestTransition(segmentId: Long, transition: Transition): Future[Unit] = {
    publisher.publish(Message.SegmentTransition(segmentId, transition))
  }

  private def propagateFailure(segmentId: Long, message: String): Future[Unit] = segmentDAO
    .outputs(segmentId)
    .flatMap { inputs =>
      logger.info(s"Propagating failure to $inputs")
      val propagate = inputs.map { input =>
        requestTransition(
          input.targetSegmentId,
          Transition.Fail("Caused by failure of parent segment", message)
        )
      }
      Future.collect(propagate).unit
    }

  private def dispatchTransformation(
    segmentId: Long
  )(implicit ctx: segmentDAO.Ctx): Future[Unit] = {
    val node = for {
      segment <- segmentDAO.byId(segmentId)
      maybeNotebook <- transformationDAO.get(segment.datasetId)
      dataset <- datasetDAO.byId(segment.datasetId)
      maybeEntity <- entityDAO.byId(dataset.ownerId)
      cluster <- clusterDAO.byOwner(dataset.ownerId)
    } yield (segment, maybeNotebook, dataset, maybeEntity, cluster.headOption)

    node.flatMap {
      case (segment, Some(notebookId), dataset, Some(owner), Some(cluster)) =>
        val (bucket, key) = cloudService.allocatePath(owner, dataset, segment)
        val path = s"s3a://$bucket/$key"
        val transformationId = UUID.randomUUID().toString
        logger.info(
          s"Dispatching transformation [$transformationId] in ${owner.name}/${cluster.tag} to generate ${owner.name}/${dataset.tag}@${segment.version}"
        )
        transformClient
          .transform(
            notebookId,
            cluster.id,
            transformationId,
            path,
            owner.name,
            dataset.tag,
            segment.version
          )
          .onSuccess { artifact => logger.info(s"Transformation artifact: $artifact") }
          .unit

      case (_, None, _, _, _) => Future.exception(new Exception(s"No generating notebook for segment $segmentId"))
      case (_, _, _, _, None) => Future.exception(new Exception(s"No cluster found to generate segment $segmentId"))
      case _ => Future.exception(new Exception(s"Unable to transform $segmentId"))
    }
  }

  private def activateWaiters(
    segmentId: Long
  )(implicit ctx: segmentDAO.Ctx): Future[Unit] = {
    segmentDAO.outputs(segmentId).flatMap { outputs =>
      val readySegments = outputs
        .map { output =>
          logger.info(s"Checking if segment ${output.targetSegmentId} can be activated")

          segmentDAO.byId(output.targetSegmentId).flatMap { segment =>
            if (segment.state == SegmentState.Awaiting) {
              segmentDAO.inputs(segment.id).flatMap { inputs =>
                Future
                  .collect(inputs.map(_.sourceSegmentId).map(segmentDAO.byId))
                  .map { inputs =>
                    logger.info(s"Segment ${segment.id} inputs: $inputs")
                    if (inputs.forall(_.state == SegmentState.Materialized)) {
                      Some(segment.id)
                    } else {
                      None
                    }
                  }
              }
            } else {
              Future.value(None)
            }
          }

        }
      Future
        .collect(readySegments)
        .map(_.flatten)
        .flatMap { segmentIds =>
          Future.join(segmentIds.map { id => requestTransition(id, Transition.Enqueue()) })
        }
    }
  }

  private def materializeTree(
    segmentId: Long
  )(implicit ctx: segmentDAO.Ctx): Future[Unit] = {
    val waitingOn = segmentDAO.inputs(segmentId).flatMap { inputs =>
      Future.collect(inputs.map(_.sourceSegmentId).map(segmentDAO.byId)).map { dependencies =>
        logger.info(s"Segment $segmentId takes as input: $dependencies")
        dependencies.filter(_.state != SegmentState.Materialized)
      }
    }
    waitingOn.flatMap { dependencies =>
      logger.info(s"Segment $segmentId is waiting on: $dependencies")
      if (dependencies.isEmpty) {
        requestTransition(segmentId, Transition.Enqueue())
      } else {
        Future.join(
          dependencies
            .filter(_.state == SegmentState.Announced)
            .map(_.id)
            .map(dependency => requestTransition(dependency, Transition.Await(Trigger.Downstream(segmentId))))
        )
      }
    }
  }

  private def propagateAnnouncement(
    segmentId: Long
  )(implicit ctx: segmentDAO.Ctx): Future[Unit] = {
    segmentDAO.byId(segmentId).flatMap { segment =>
      graphDAO
        .out(segment.datasetId)
        .flatMap { outEdges =>
          logger.info(s"Out edges for dataset ${segment.datasetId}: $outEdges")

          val children = outEdges
            .filter(_.valid)
            .filter(_.inputMode == InputMode.Trigger)
            .map(_.targetDatasetId)
            .distinct
          val propagate = children.map(datasetId => generateNewSegment(datasetId, Trigger.Upstream(segmentId)))
          Future.join(propagate)
        }
    }
  }

  private def generateNewSegment(
    datasetId: Long,
    trigger: Trigger
  )(implicit ctx: segmentDAO.Ctx): Future[Unit] = {
    logger.info(s"Checking dataset $datasetId for potential new segments")

    graphDAO
      .in(datasetId)
      .flatMap { inEdges =>
        logger.info(s"In edges for dataset $datasetId: $inEdges")
        if (inEdges.forall(_.valid)) {
          val inputs = inEdges.map { edge =>
            val sourceDatasetId = edge.sourceDatasetId
            logger.info(s"Found in edge: $edge")

            val selectInputSegments = segmentDAO.list(sourceDatasetId).map { segments =>
              logger.info(s"Found ${segments.size} input candidates from $sourceDatasetId")
              segments.maxByOption(_.version).toSeq
            }

            selectInputSegments
              .map(_.sortBy(_.version))
              .map { segments =>
                segments.map { segment => edge.binding -> segment.id }
              }
          }
          Future
            .collect(inputs)
            .map(_.flatten)
            .map(Some(_))
        } else {
          logger.info(s"Skipping due to invalid edge")
          Future.None
        }
      }
      .flatMap {
        case Some(segments) =>
          for {
            segmentId <- segmentDAO.make(datasetId)
            _ <- segmentDAO.addInputs(segmentId, segments.toMap)
            _ <- requestTransition(segmentId, Transition.Announce(trigger))
          } yield ()
        case _ => Future.Done
      }
  }
}

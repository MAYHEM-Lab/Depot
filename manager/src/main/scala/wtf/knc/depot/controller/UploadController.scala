package wtf.knc.depot.controller

import java.util.UUID

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.Request
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.finatra.http.request.RequestUtils
import com.twitter.io.{Buf, BufInputStream, Pipe}
import com.twitter.util.jackson.ScalaObjectMapper
import com.twitter.util._
import javax.inject.{Inject, Provider, Singleton}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.io.{BytesProgressWatcher, ProgressMonitoredInputStream}
import org.jets3t.service.model.{MultipartUpload, S3Object}
import org.jets3t.service.utils.Mimetypes
import wtf.knc.depot.controller.UploadController.{CreateUpload, FindUpload, UploadProgress, UploadResponse}
import wtf.knc.depot.dao._
import wtf.knc.depot.model._

object UploadController {
  private case class CreateUpload(
    @RouteParam entityName: String,
    parts: Int
  ) extends EntityRoute

  private case class FindUpload(
    @RouteParam entityName: String,
    @RouteParam filename: String,
    uploadId: String
  ) extends EntityRoute

  private case class UploadResponse(
    id: String,
    filename: String
  )

  private case class UploadProgress(
    bytesTransferred: Long
  )

}

@Singleton
class UploadController @Inject() (
  override val authProvider: Provider[Option[Auth]],
  override val clusterDAO: ClusterDAO,
  override val entityDAO: EntityDAO,
  objectMapper: ScalaObjectMapper,
  s3Pool: FuturePool,
  s3: RestS3Service
) extends Controller
  with EntityRequests
  with Authentication {

  private def path(implicit req: EntityRoute): Future[String] = entity(Some(Role.Owner)).map {
    case Entity.User(_, name, _) => name
    case Entity.Organization(_, name, _) => name
  }

  private def upload(implicit req: FindUpload): Future[MultipartUpload] = path.map { pref =>
    val bucket = "_uploads"
    val path = s"$pref/${req.filename}"
    new MultipartUpload(req.uploadId, bucket, path)
  }

  prefix("/api/entity/:entity_name/files") {

    post("/?") { implicit req: CreateUpload =>
      path.flatMap { pref =>
        val filename = s"${UUID.randomUUID().toString}-${Time.now.inMillis}"

        val bucket = "_uploads"
        val path = s"$pref/$filename"

        for {
          multipart <- s3Pool { s3.multipartStartUpload(bucket, path, null) }
          // The first multipart upload to Eucalyptus S3 fails for some reason.
          // Eat the failure on the server-side instead of browser.
          _ <- s3Pool { s3.multipartUploadPart(multipart, 1, new S3Object(path, Array.empty[Byte])) }
        } yield response.accepted(UploadResponse(multipart.getUploadId, filename))
      }
    }

    prefix("/:filename") {
      put("/?") { implicit req: Request =>
        require(req.contentLength.isDefined)
        val contentLength = req.contentLength.get
        val progressMonitor = new BytesProgressWatcher(contentLength)

        val entityName = req.params("entity_name")
        val uploadId = req.params("upload_id")
        val filename = req.params("filename")
        val partNumber = req.params("part_number").toInt

        val pipe = new Pipe[String]

        val (activity, witness) = Activity[Option[UploadProgress]]()
        activity.values.respond {
          case Return(Some(progress)) =>
            pipe.write(objectMapper.writeValueAsString(progress))

          case Return(None) =>
            logger.info(s"Upload part $partNumber for $uploadId [$filename] completed")
            pipe.write("data: {\"complete\": true}\n\n").before { pipe.close() }

          case Throw(ex) =>
            logger.info(s"Failed to upload part $partNumber for $uploadId [$filename]", ex)
            pipe.write("data: {\"error\": true}\n\n").before { pipe.close() }
        }

        val stream = pipe.map(_ + "\n\n").map(Buf.Utf8(_))

        upload(FindUpload(entityName, filename, uploadId))
          .flatMap { multipart =>
            val multiparams = RequestUtils.multiParams(req)
            println(req.content.length)
            println(multiparams)
            val obj = new S3Object(multipart.getObjectKey) {
              setContentLength(contentLength)
              setContentType(Mimetypes.MIMETYPE_OCTET_STREAM)
              setDataInputStream {
                val stream = if (req.isChunked) {
                  new BufInputStream(req.content)
                } else {
                  new BufInputStream(req.content)
                }
                val pis = new ProgressMonitoredInputStream(stream, progressMonitor)
                pis
              }
            }
            val upload = s3Pool { val part = s3.multipartUploadPart(multipart, partNumber, obj) }
              .onSuccess { _ => witness.notify(Return(None)) }
              .onFailure { ex => witness.notify(Throw(ex)) }

            Future.whileDo(!upload.isDefined) {
              val progress = UploadProgress(progressMonitor.getBytesTransferred)
              witness.notify(Return(Some(progress)))
              Future.sleep(250.millis)(DefaultTimer)
            }
            upload.unit
          }
          .ensure { pipe.close() }
        response.streaming(
          stream,
          headers = Map(
            "Content-Type" -> Seq("text/event-stream"),
            "Connection" -> Seq("keep-alive"),
            "Cache-Control" -> Seq("no-cache")
          )
        )
      }

      post("/commit") { implicit req: FindUpload =>
        upload
          .flatMap { multipart =>
            s3Pool { s3.multipartCompleteUpload(multipart) }
          }
          .map(_ => response.created)
      }

      delete("/?") { implicit req: FindUpload =>
        upload
          .flatMap { multipart =>
            val delete = s3Pool { s3.deleteObject(multipart.getBucketName, multipart.getObjectKey) }
            val abort = s3Pool { s3.multipartAbortUpload(multipart) }
            Future.join(delete, abort)
          }
          .map(_ => response.noContent)
      }
    }
  }
}

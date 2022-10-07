package wtf.knc.depot.controller

import java.net.URL
import java.util.UUID

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.{Fields, MediaType}
import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.inject.annotations.Flag
import com.twitter.util._
import com.twitter.util.jackson.ScalaObjectMapper
import javax.inject.{Inject, Provider, Singleton}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.{MultipartUpload, S3Object}
import wtf.knc.depot.controller.UploadController.{CreateUpload, FindUpload, UploadResponse}
import wtf.knc.depot.dao._
import wtf.knc.depot.model._

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

object UploadController {
  private case class CreateUpload(
    @RouteParam entityName: String,
    parts: Int,
    contentType: Option[String]
  ) extends EntityRoute

  private case class FindUpload(
    @RouteParam entityName: String,
    @RouteParam filename: String,
    uploadId: String
  ) extends EntityRoute

  private case class UploadResponse(
    id: String,
    filename: String,
    parts: Seq[String]
  )

  private case class UploadProgress(
    transferred: Long
  )

}

@Singleton
class UploadController @Inject() (
  @Flag("deployment.id") deployment: String,
  override val authProvider: Provider[Option[Auth]],
  override val clusterDAO: ClusterDAO,
  override val entityDAO: EntityDAO,
  objectMapper: ScalaObjectMapper,
  s3Pool: FuturePool,
  s3: RestS3Service
) extends Controller
  with EntityRequests
  with Authentication {
  private final val UploadBucket = s"$deployment.uploads"
  private final val ContentTypeMeta = "Depot-Content-Type"

  private def path(implicit req: EntityRoute): Future[String] = entity(Some(Role.Owner)).map(_.name)

  private def upload(implicit req: FindUpload): Future[MultipartUpload] = path.map { pref =>
    val path = s"$pref/${req.filename}"
    new MultipartUpload(req.uploadId, UploadBucket, path)
  }

  prefix("/api/entity/:entity_name/files") {

    post("/?") { implicit req: CreateUpload =>
      path.flatMap { pref =>
        val filename = s"${UUID.randomUUID().toString}-${Time.now.inMillis}"

        val path = s"$pref/$filename"
        val headers = req.contentType match {
          case Some(contentType) => Map[String, Object](Fields.ContentType -> contentType)
          case _ => Map.empty[String, Object]
        }

        val metadata = Map[String, Object](
          ContentTypeMeta -> headers.getOrElse(Fields.ContentType, MediaType.OctetStream)
        )
        for {
          multipart <- s3Pool { s3.multipartStartUpload(UploadBucket, path, metadata.asJava) }
          urls <- s3Pool {
            val expiry = (Time.now + 1.hour).inMillis
            1 to req.parts map { i =>
              val s3url = s3.createSignedUrl(
                "PUT",
                UploadBucket,
                path,
                s"partNumber=$i&uploadId=${multipart.getUploadId}",
                headers.asJava,
                expiry,
                false
              )
              val url = new URL(s3url)
              s"/upload${url.getPath}?${url.getQuery}"
            }
          }
          // The first multipart upload to Eucalyptus S3 fails for some reason.
          // Eat the failure on the server-side instead of browser.
          _ <- s3Pool { s3.multipartUploadPart(multipart, 1, new S3Object(path, Array.empty[Byte])) }
        } yield response.accepted(UploadResponse(multipart.getUploadId, filename, urls))
      }
    }

    prefix("/:filename") {
      post("/?") { implicit req: FindUpload =>
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
            Future
              .join(delete, abort)
              .handle { case NonFatal(ex) => logger.warn(s"Failed to clean up upload", ex) }
          }
          .map(_ => response.noContent)
      }
    }
  }
}

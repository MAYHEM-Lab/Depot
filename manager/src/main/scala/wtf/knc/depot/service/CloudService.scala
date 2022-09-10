package wtf.knc.depot.service

import java.io.FileOutputStream
import java.nio.file.Files
import java.util.UUID
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finatra.utils.FuturePools
import com.twitter.inject.annotations.Flag
import com.twitter.util.logging.Logging
import com.twitter.util._
import javax.inject.{Inject, Singleton}
import org.apache.commons.codec.digest.DigestUtils
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.model.CORSConfiguration.AllowedMethod
import org.jets3t.service.model.{CORSConfiguration, S3Object}
import wtf.knc.depot.model.{Dataset, Entity, Segment, Visibility}

import scala.jdk.CollectionConverters._
import scala.sys.process._
import scala.util.Using

trait CloudService {
  def allocatePath(entity: Entity, dataset: Dataset, segment: Segment): (String, String)
  def reclaimPath(path: String): Future[Unit]
  def size(path: String): Future[Long]
  def downloadPath(path: String): Future[String]
  def createDataset(entityName: String, datasetTag: String): Future[Unit]
  def createEntity(name: String): Future[(String, String)]
  def addMember(user: String, entity: String): Future[Unit]
  def removeMember(user: String, entity: String): Future[Unit]
  def deleteEntity(name: String): Future[Unit]
  def addDatasetACL(entityName: String, datasetOwner: String, datasetTag: String): Future[Unit]
  def delDatasetACL(entityName: String, datasetOwner: String, datasetTag: String): Future[Unit]
}

/*
Currently uses a bucket per dataset. This limits us to 50 (1 private + 1 public) datasets total.
We can utilize a single bucket to store all datasets if Eucalyptus gives us sufficient IAM control:

arn:aws:s3:::my-bucket/my-dataset/\*
s3:DeleteObject
s3:PutObject
s3:HeadObject
s3:GetObject

arn:aws:s3:::my-bucket/my-dataset
s3:ListBucket
 */

@Singleton
class EucalyptusCloudService @Inject() (
  @Flag("deployment.id") deployment: String,
  @Flag("euca.iam.url") iamUrl: String,
  @Flag("euca.admin.access_key") adminAccessKey: String,
  @Flag("euca.admin.secret_key") adminSecretKey: String,
  s3: RestS3Service,
  s3Pool: FuturePool
) extends CloudService
  with Logging {
  private final val ProcessExecutorPool = FuturePools.unboundedPool("process-executor-pool")
  private final val AuthFlags = s"-I $adminAccessKey -S $adminSecretKey -U $iamUrl"
  private final val PublicReaders = s"$deployment.public.readers"
  private final val UploadBucket = s"$deployment.uploads"
  private final val BundleBucket = s"$deployment.bundles"

  private final val BundleMetaKey = "Depot-In-Progress"
  private final val BundleTTL = 5.minutes
  private final val BundlePoll = 30.seconds

  private final val BucketCors = {
    val cors = new CORSConfiguration()
    cors.addCORSRule(
      cors.newCORSRule(
        "*",
        Set(AllowedMethod.HEAD, AllowedMethod.GET, AllowedMethod.PUT, AllowedMethod.POST).asJava,
        Set("*").asJava
      )
    )
    cors
  }

  private final val WriteFlags = "-a 's3:*'"
  private final val ReadFlags = Seq("ListBucket", "GetObject", "HeadObject", "GetObjectLocation")
    .map(p => s"-a 's3:$p'")
    .mkString(" ")

  private def iamUserName(entityName: String) = s"$deployment.entity.$entityName"
  private def iamGroupName(entityName: String) = s"$deployment.entity.$entityName"
  private def aclGroupName(entityName: String, datasetTag: String) = s"$deployment.shared.$entityName.$datasetTag"
  private def publicBucketName(entityName: String, datasetTag: String) =
    s"$deployment.public-$entityName-$datasetTag".toLowerCase
  private def privateBucketName(entityName: String, datasetTag: String) =
    s"$deployment.private-$entityName-$datasetTag".toLowerCase

  Await.result {
    s3Pool {
      s3.createBucket(UploadBucket)
      s3.setBucketCORSConfiguration(UploadBucket, BucketCors)
      s3.createBucket(BundleBucket)
      s3.setBucketCORSConfiguration(BundleBucket, BucketCors)
    }
  }

  override def allocatePath(entity: Entity, dataset: Dataset, segment: Segment): (String, String) = {
    val id = UUID.randomUUID().toString
    val bucketFn = dataset.visibility match {
      case Visibility.Public => publicBucketName _
      case Visibility.Private => privateBucketName _
    }
    val bucket = bucketFn(entity.name, dataset.tag)

    (bucket, s"segments/${segment.version}/$id".toLowerCase)
  }

  override def reclaimPath(path: String): Future[Unit] = s3Pool {
    val location = path.substring(6)
    val Array(bucket, parts) = location.split("/", 2)
    val objects = s3.listObjects(bucket, parts, null)
    s3.deleteMultipleObjects(bucket, objects.map(_.getKey))
  }

  private def consolidateDownload(filename: String): Future[Boolean] = {
    s3Pool { Try(s3.getObjectDetails(BundleBucket, filename)).toOption }
      .flatMap {
        case Some(existingObj) =>
          val maybeInProgress = Option(existingObj.getMetadata(BundleMetaKey))
            .map(_.asInstanceOf[String])
            .map(_.toLong)

          maybeInProgress match {
            case Some(lastUpdate) if Time.now.inMillis - lastUpdate < BundleTTL.inMillis =>
              logger.info(s"Consolidated bundle $filename is still in progress")
              Future.sleep(BundlePoll)(DefaultTimer).before { consolidateDownload(filename) }
            case Some(_) =>
              logger.info(s"Consolidated bundle $filename has expired")
              Future.False
            case None =>
              logger.info(s"Consolidated bundle $filename has completed")
              Future.True
          }
        case _ =>
          logger.info(s"Bundle $filename does not exist")
          Future.False
      }
  }

  override def downloadPath(path: String): Future[String] = {
    val location = path.substring(6)
    val Array(bucket, parts) = location.split("/", 2)

    val filename = DigestUtils.md5Hex(path)
    val targetFile = s"$filename.zip"
    logger.info(s"Received request to bundle path $path as $targetFile")

    consolidateDownload(targetFile)
      .flatMap {
        if (_) { Future.Done }
        else {
          logger.info(s"Starting bundle of path $path as $targetFile")
          s3Pool {
            val metaObject = new S3Object(targetFile, Array.empty[Byte])
            metaObject.addMetadata(BundleMetaKey, Time.now.inMillis.toString)
            s3.putObject(BundleBucket, metaObject)

            val zipFile = Files.createTempFile(filename, ".zip").toFile

            Using.Manager { use =>
              val fos = use(new FileOutputStream(zipFile))
              val zos = use(new ZipOutputStream(fos))

              s3
                .listObjects(bucket, parts + "/", "/")
                .foreach { obj =>
                  logger.info(s"Adding object ${obj.getKey} to bundle $targetFile")
                  Using(s3.getObject(bucket, obj.getKey).getDataInputStream) { is =>
                    zos.putNextEntry(new ZipEntry(obj.getKey.split('/').last))
                    val buf = new Array[Byte](50 * 1024 * 1024)
                    Iterator
                      .continually(is.read(buf))
                      .takeWhile(_ != -1)
                      .foreach(read => zos.write(buf, 0, read))
                    zos.closeEntry()
                  }
                  logger.info(s"Added object ${obj.getKey} to bundle $targetFile")
                }
              zos.flush()
              fos.flush()
            }
            logger.info(s"Uploading bundle $targetFile")
            val uploadObject = new S3Object(zipFile)
            uploadObject.setKey(targetFile)
            s3.putObject(BundleBucket, uploadObject)
            logger.info(s"Uploaded bundle $targetFile")
          }
        }
      }
      .map { _ => s3.createSignedGetUrl(BundleBucket, targetFile, (Time.now + 1.hour).toDate) }
  }

  override def size(path: String): Future[Long] = s3Pool {
    val location = path.substring(6)
    val Array(bucket, parts) = location.split("/", 2)
    s3
      .listObjects(bucket, parts + "/", "/")
      .map { _.getMetadata("Content-Length").asInstanceOf[String].toLong }
      .sum
  }

  override def addDatasetACL(entityName: String, datasetOwner: String, datasetTag: String): Future[Unit] =
    ProcessExecutorPool {
      val iamGroup = iamGroupName(entityName)
      val privateBucket = privateBucketName(datasetOwner, datasetTag)
      s"euare-groupaddpolicy $AuthFlags -p read_bucket.$privateBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$privateBucket' $iamGroup".!!
      s"euare-groupaddpolicy $AuthFlags -p read_object.$privateBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$privateBucket/*' $iamGroup".!!
    }

  override def delDatasetACL(entityName: String, datasetOwner: String, datasetTag: String): Future[Unit] =
    ProcessExecutorPool {
      val iamGroup = iamGroupName(entityName)
      val privateBucket = privateBucketName(datasetOwner, datasetTag)
      s"euare-groupdelpolicy $AuthFlags -p read_bucket.$privateBucket $iamGroup".!!
      s"euare-groupdelpolicy $AuthFlags -p read_object.$privateBucket $iamGroup".!!
    }

  override def createDataset(entityName: String, datasetTag: String): Future[Unit] = {
    val publicBucket = publicBucketName(entityName, datasetTag)
    val privateBucket = privateBucketName(entityName, datasetTag)
    val aclGroup = aclGroupName(entityName, datasetTag)
    val iamGroup = iamGroupName(entityName)
    s3Pool {
      s3.getOrCreateBucket(publicBucket)
      s3.setBucketCORSConfiguration(publicBucket, BucketCors)
      s3.getOrCreateBucket(privateBucket)
      s3.setBucketCORSConfiguration(privateBucket, BucketCors)
    }.flatMap { _ =>
      ProcessExecutorPool {
        s"euare-groupcreate $AuthFlags $aclGroup".!!

        s"euare-groupaddpolicy $AuthFlags -p read_bucket.$publicBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$publicBucket' $PublicReaders".!!
        s"euare-groupaddpolicy $AuthFlags -p read_object.$publicBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$publicBucket/*' $PublicReaders".!!

        s"euare-groupaddpolicy $AuthFlags -p read_bucket.$privateBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$privateBucket' $iamGroup".!!
        s"euare-groupaddpolicy $AuthFlags -p read_object.$privateBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$privateBucket/*' $iamGroup".!!

        s"euare-groupaddpolicy $AuthFlags -p read_bucket.$privateBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$privateBucket' $aclGroup".!!
        s"euare-groupaddpolicy $AuthFlags -p read_object.$privateBucket -e Allow $ReadFlags -r 'arn:aws:s3:::$privateBucket/*' $aclGroup".!!

        s"euare-useraddpolicy $AuthFlags -p write_bucket.$privateBucket -e Allow $WriteFlags -r 'arn:aws:s3:::$privateBucket' $iamGroup".!!
        s"euare-useraddpolicy $AuthFlags -p write_object.$privateBucket -e Allow $WriteFlags -r 'arn:aws:s3:::$privateBucket/*' $iamGroup".!!

        s"euare-useraddpolicy $AuthFlags -p write_bucket.$publicBucket -e Allow $WriteFlags -r 'arn:aws:s3:::$publicBucket' $iamGroup".!!
        s"euare-useraddpolicy $AuthFlags -p write_object.$publicBucket -e Allow $WriteFlags -r 'arn:aws:s3:::$publicBucket/*' $iamGroup".!!
      }
    }
  }

  override def createEntity(name: String): Future[(String, String)] = ProcessExecutorPool {
    val iamUser = iamUserName(name)
    val iamGroup = iamGroupName(name)

    val Array(accessKey, secretKey) = s"euare-usercreate $AuthFlags $iamUser -k".!!.split("\n")
    logger.info(s"Created Eucalyptus user for $name with access key $accessKey")
    s"euare-groupcreate $AuthFlags $iamGroup".!!
    logger.info(s"Created Eucalyptus group for $name")

    try {
      s"euare-groupcreate $AuthFlags $PublicReaders".!!
      logger.info(s"Created public dataset readers group")
    } catch { case _: Throwable => }

    s"euare-groupadduser $AuthFlags -u $iamUser $PublicReaders".!!
    logger.info(s"Added $name to public dataset readers")

    s"euare-groupadduser $AuthFlags -u $iamUser $iamGroup".!!
    logger.info(s"Added $name to group $name")

    (accessKey, secretKey)
  }

  override def addMember(user: String, entity: String): Future[Unit] = ProcessExecutorPool {
    val iamUser = iamUserName(user)
    val iamGroup = iamGroupName(entity)
    s"euare-groupadduser $AuthFlags -u $iamUser $iamGroup".!!
  }

  override def removeMember(user: String, entity: String): Future[Unit] = ProcessExecutorPool {
    val iamUser = iamUserName(user)
    val iamGroup = iamGroupName(entity)
    s"euare-groupremoveuser $AuthFlags -u $iamUser $iamGroup".!!
  }

  override def deleteEntity(name: String): Future[Unit] = ProcessExecutorPool {
    val iamUser = iamUserName(name)
    val iamGroup = iamGroupName(name)
    s"euare-groupdel -r $AuthFlags $iamGroup".!!
    s"euare-userdel -r $AuthFlags $iamUser".!!
  }
}

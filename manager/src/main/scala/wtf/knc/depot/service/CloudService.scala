package wtf.knc.depot.service

import java.util.UUID

import com.twitter.finatra.utils.FuturePools
import com.twitter.inject.annotations.Flag
import com.twitter.util.logging.Logging
import com.twitter.util.{Await, Future, FuturePool}
import javax.inject.{Inject, Singleton}
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import wtf.knc.depot.model.{Dataset, Entity, Segment, Visibility}

import scala.sys.process._

trait CloudService {
  def allocatePath(entity: Entity, dataset: Dataset, segment: Segment): (String, String)
  def reclaimPath(path: String): Future[Unit]
  def size(path: String): Future[Long]
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
    s3Pool { s3.createBucket(s"$deployment.uploads") }
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
      s3.getOrCreateBucket(privateBucket)
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

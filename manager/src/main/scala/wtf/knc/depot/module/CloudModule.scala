package wtf.knc.depot.module

import java.net.URL

import com.google.inject.Provides
import com.twitter.finatra.utils.FuturePools
import com.twitter.inject.TwitterModule
import com.twitter.inject.annotations.Flag
import com.twitter.util.FuturePool
import javax.inject.Singleton
import org.apache.http.impl.client.HttpClients
import org.jets3t.service.Jets3tProperties
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.security.AWSCredentials
import wtf.knc.depot.service.{CloudService, EucalyptusCloudService}

object CloudModule extends TwitterModule {
  flag[String](
    "deployment.id",
    "test",
    "ID of the Depot deployment"
  )

  flag[String](
    "euca.iam.url",
    "http://iam.cloud.aristotle.ucsb.edu:8773/",
    "Eucalyptus IAM path"
  )

  flag[String](
    "euca.s3.url",
    "http://s3.cloud.aristotle.ucsb.edu:8773/",
    "Eucalyptus S3 path"
  )

  flag[String](
    "euca.admin.access_key",
    "Eucalyptus admin access key"
  )

  flag[String](
    "euca.admin.secret_key",
    "Eucalyptus admin secret key"
  )

  override def configure(): Unit = {
    bind[CloudService].to[EucalyptusCloudService]
  }

  @Provides
  @Singleton
  def s3Pool: FuturePool = {
    val pool = FuturePools.fixedPool("s3-request-pool", 1)
    onExit(pool.executor.shutdown())
    pool
  }

  @Provides
  @Singleton
  def s3(
    @Flag("euca.s3.url") s3Url: String,
    @Flag("euca.admin.access_key") adminAccessKey: String,
    @Flag("euca.admin.secret_key") adminSecretKey: String
  ): RestS3Service = {
    val url = new URL(s3Url)
    val props = new Jets3tProperties()
    props.setProperty("httpclient.proxy-autodetect", "false");
    props.setProperty("s3service.s3-endpoint", url.getHost);
    props.setProperty("s3service.s3-endpoint-http-port", url.getPort.toString);
    props.setProperty("s3service.https-only", "false");
    props.setProperty("s3service.disable-dns-buckets", "true");
    props.setProperty("storage-service.request-signature-version", "AWS2");
    val s3 = new RestS3Service(new AWSCredentials(adminAccessKey, adminSecretKey), null, null, props)
    val httpClient = HttpClients.createDefault()
    onExit(httpClient.close())
    s3.setHttpClient(httpClient)
    s3
  }
}

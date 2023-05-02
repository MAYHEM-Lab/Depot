package wtf.knc.depot.service

import javax.inject.{Inject, Singleton}

@Singleton
class Validator @Inject() () {
  private final val TagRegex = ""
  private final val FilenameRegex = ""

  def validateDataset(): Unit = ()
  def validateEntity(): Unit = ()

  def validateFilename(name: String): Unit = {
    // Only alphanumerics,

  }
}

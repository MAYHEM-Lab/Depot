package wtf.knc.depot.module

import com.twitter.inject.TwitterModule
import wtf.knc.depot.notebook.{CloudNotebookStore, NotebookStore}

object NotebookModule extends TwitterModule {
  override def configure(): Unit = {
    bind[NotebookStore].to[CloudNotebookStore]
  }
}

package wtf.knc.depot.module

import com.twitter.inject.TwitterModule
import com.twitter.inject.requestscope.RequestScopeBinding
import wtf.knc.depot.controller.Auth

object AuthModule extends TwitterModule with RequestScopeBinding {
  override def configure(): Unit = {
    bindRequestScope[Option[Auth]]
  }
}

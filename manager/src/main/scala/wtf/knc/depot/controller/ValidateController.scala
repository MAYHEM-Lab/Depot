package wtf.knc.depot.controller

import com.twitter.finatra.http.Controller
import com.twitter.finatra.http.annotations.RouteParam
import com.twitter.inject.Logging
import javax.inject.{Inject, Singleton}
import wtf.knc.depot.controller.ValidateController.{ValidateRequest, ValidateResponse}
import wtf.knc.depot.dao.{DatasetDAO, NotebookDAO, EntityDAO}

object ValidateController {
  case class ValidateRequest(@RouteParam text: String)
  case class ValidateResponse(valid: Boolean)
}

@Singleton
class ValidateController @Inject() (
  userDAO: EntityDAO,
  datasetDAO: DatasetDAO,
  notebookDAO: NotebookDAO
) extends Controller
  with Logging {

  prefix("/api/validate") {
    get("/username/:text") { req: ValidateRequest =>
      userDAO.byName(req.text).map { maybe =>
        response.ok(ValidateResponse(maybe.isEmpty))
      }
    }

    get("/dataset/:text") { req: ValidateRequest =>
      datasetDAO.byTag(req.text).map { maybe =>
        response.ok(ValidateResponse(maybe.isEmpty))
      }
    }

    get("/notebook/:text") { req: ValidateRequest =>
      notebookDAO.byTag(req.text).map { maybe =>
        response.ok(ValidateResponse(maybe.isEmpty))
      }
    }

  }
}

# modified AuthBackendBasic to allow sessions
#' @import RestRserve
#' @export
SentryBackend <- R6::R6Class('SentryBackend',
                             inherit = AuthBackendBasic,
                             public = list(
                               authenticate = function(request, response) {
                                  mySid <- request$cookies[['sid']]
                                  if (!is.null(mySid)){  # if we have a sid
                                   myUser <- request$cookies[['user']]
                                   res <- super$auth_fun(user = myUser, sid = mySid)
                                   if (isTRUE(res)) {
                                     return(TRUE)
                                   } else {
                                     raise(self$HTTPError$unauthorized(
                                       body = "401 Invalid Username/Password",
                                       headers = list("WWW-Authenticate" = "Basic"))
                                     )
                                   }
                                  } else { # no sid, this is the login
                                    super$authenticate(request, response)
                                  }
                               }

                            )
)

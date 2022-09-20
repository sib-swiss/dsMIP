# modified AuthBackendBasic to allow sessions
# the auth_fun's signature must be (user, password = NULL, sid = NULL) and it must always return the sid if it worked or NULL if it didn't
#' @import RestRserve
#' @export
SentryBackend <- R6::R6Class('SentryBackend',
                             inherit = AuthBackendBasic,
                             public = list(
                               authenticate = function(request, response) {
                                  mySid <- request$cookies[['sid']]
                                  res <- NULL
                                  if (!is.null(mySid)){  # if we have a sid
                                    myUser <- request$cookies[['user']]
                                    res <- super$auth_fun(user = myUser, password = NULL, sid = mySid)
                                  } else { # no sid, this is the login
                                    user_password = private$extract_credentials(request, response)
                                    myUser <- user_password[[1]]
                                    res <- super$auth_fun(myUser, user_password[[2]], sid = NULL)
                                    mySid <- res
                                  }
                                  if (!is.null(res)) {  # auth_fun must return a sid
                                    response$set_cookie('sid', mySid) # do I set it every time?
                                    response$set_cookie('user', myUser) # do I set it every time?
                                    return(TRUE)
                                  } else {
                                    raise(self$HTTPError$unauthorized(
                                        body = "401 Invalid Username/Password",
                                        headers = list("WWW-Authenticate" = "Basic"))
                                      )
                                  }
                                }
                             )
)

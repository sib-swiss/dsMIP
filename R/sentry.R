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
                                  }
                                  if(res == 'unauthorized'){
                                    raise(self$HTTPError$unauthorized(
                                      body = "401 Invalid Username/Password",
                                      headers = list("WWW-Authenticate" = "Basic"))
                                    )
                                  } else if(grepl('timeout', res)){
                                    raise(self$HTTPError$unauthorized(
                                      body = paste0("401 Session Expired, ", res),
                                      headers = list("WWW-Authenticate" = "Basic"))
                                    )
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


# on second thoughts, here's an auth fun generator, must be called in the app with
#  mySentryFunction <- makeSEntryFunction(somequeue, someotherpath, sometimeout)

# closure to create a sentry function with a specified request queue and response path
#'@export
makeSentryFunction <- function(requestQ, responsePath, timeout = 1800){

  sentryFunc <-  function(user, password = NULL, sid = NULL ){ # must return a sid
    if(is.null(sid)){ # we must login
      if(is.null(password)){ # don't even
        return('unauthorized')
      }
      newSid <- paste0(runif(1), Sys.time()) %>% digest
      newPath <- paste0(responsePath, '/', user,'_', newSid) # new pipes in here starting with 1

      # send the login command to the listener(s)
      mesg <- list(fun = 'authLogin', args = list(user, password))
      logged <- qCommand(reqQ, newPath, message = mesg, wait = TRUE, timeout = 60)
      if(logged$message == 'OK'){
        return(newSid)
      } else {
        return('unauthorized')
      }
    } else {  # we have a sid, handle timeouts
      myPath <- paste0(responsePath, '/', user,'_', sid)
      lastTime <- file.info(myPath)[,'ctime']
      if(is.na(lastTime) ){ # if it doesn't exist
        return('unauthorized')
      }
      if(as.numeric(Sys.time()) - as.numeric(lastTime) > timeout){ # too old, sorry
        unlink(myPath, recursive = TRUE, force = TRUE)
        return(paste0('timeout after ', timeout, ' seconds'))
      }

    }
  }

  return(sentryFunc)
}

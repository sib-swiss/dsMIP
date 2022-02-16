# modified AuthBackendBasic to allow sessions
#' @import RestRserve
#' @export
SentryBackend <- R6::R6Class('SentryBackend',
                             inherit = AuthBackendBasic,
                             private =  list(
                               .pipeFolder = NULL,
                               .validPipes = function(sid, user){
                                 prefix <- paste0(private$.pipeFolder, '/',user, '_', sid)
                                 pipes <- c(paste0(prefix, '.req'), paste0(prefix, '.res'))
                                 if(file.exists(pipes[1]) && file.exists(pipes[2])){
                                   return(TRUE)
                                 }
                                 return(FALSE)
                               },
                                .makeSid = function(reqPath, usr){
                                 trimExpr <- paste0(private$.pipeFolder, '/', usr, '_', '(.+?)\\.req')
                                 sub(trimExpr,'\\1', reqPath) # keep only the random part
                                }
                             ),
                             public = list(
                               authenticate = function(request, response) {

                                 mySid <- request$cookies[['sid']]

                                 if (!is.null(mySid)){
                                   myUser <- request$cookies[['user']]

                                   if(private$.validPipes(mySid, myUser)){ # we do have this session id
                                     response$set_cookie('sid', mySid) # do I set it every time?
                                     response$set_cookie('user', myUser) # do I set it every time?
                                     return(TRUE)
                                   } else { # we don't

                                     raise(self$HTTPError$unauthorized(
                                       body = "401 Invalid session ID",
                                       headers = list("WWW-Authenticate" = "Basic"))
                                     )
                                   }

                                 }
                                 # if we are here we have no session id, check the credentials:
                                 user_password = private$extract_credentials(request, response)
                                 bob <-tryCatch(
                                   private$auth_fun(user_password[[1]], user_password[[2]]),
                                   error = function(e){
                                     if(grepl('401', e$message)){
                                      raise(self$HTTPError$unauthorized(
                                         body = paste(e," --- 401 Invalid Username/Password"),
                                        headers = list("WWW-Authenticate" = "Basic")))
                                     } else if( grepl('Too many', e$message)) {
                                        raise(HTTPError$too_many_requests())
                                     } else {
                                       stop(e)
                                     }
                                   }

                                  )  # bob the minion is ready now
                                 # if no error above, we have a new sid, let everybody know:
                                 mySid <- private$.makeSid(bob$reqQ$path(),  bob$userName)
                                 myUser <- bob$userName
                                 request$cookies$sid <-  mySid
                                 request$cookies$user <- myUser
                                 # ugly workaround, request is locked so send bob down to the handler as a cookie
                                 request$cookies$minion <- bob
                                 response$set_cookie('sid', mySid)
                                 response$set_cookie('user', bob$userName)
                               }, # authenticate
                               initialize = function(folder, ...){
                                 private$.pipeFolder <- paste0(tempdir(TRUE), '/', folder)
                                 super$initialize(...)
                               }
                             )
)

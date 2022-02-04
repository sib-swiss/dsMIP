#' @import txtq
Despicable <- R6Class('Despicable',
                      private = list(
                        .reqQ = NULL,
                        .resQ = NULL
                        ),
                     active = list(
                       reqQ = function(value) {
                         if (missing(value)) {
                           private$.reqQ
                         } else {
                           if(!is.null(value) && !any(grepl("R6_txtq", class(value)))){
                             stop('Value must be either a R6_txtq object or NULL.')
                           }
                           if(!is.null(private$reqQ)){
                             private$.reqQ$destroy
                           }
                           private$.reqQ <- value
                         }
                       },
                       resQ = function(value) {
                         if (missing(value)) {
                           private$.resQ
                         } else {
                           if(!is.null(value) && !any(grepl("R6_txtq", class(value)))){
                             stop('Value must be either a R6_txtq object or NULL.')
                           }
                           if(!is.null(private$resQ)){
                             private$.resQ$destroy
                           }
                             private$.resQ <- txtq(value)
                         }
                       }

                     ),

                      public = list(
                        initialize = function(){
                          self$startQueues()
                        },
                        startQueues = function(reqPath = tempfile(pattern = '', tmpdir = '/tmp'), resPath = tempfile(pattern = '', tmpdir = '/tmp')){
                          private$.reqQ <- txtq(reqPath)
                          private$.resQ <- txtq(resPath)
                        },
                        stopQueues = function(){
                          if(!is.null(private$reqQ)){
                            private$.reqQ$destroy
                          }
                          if(!is.null(private$resQ)){
                            private$.resQ$destroy
                          }
                          private$.resQ <-NULL
                          private$.reqQ <-NULL
                        },
                        blockingRead = function(queue = private$.resQ, timeout = 60, every = 1){
                          st <- as.numeric(Sys.time())
                          while(TRUE){
                            msg <- queue$pop(1)
                            if(nrow(msg) == 0){
                              if(as.numeric(Sys.time()) - st > timeout){
                                stop(paste0('Timeout while waiting to read from ', queue$path()))
                              }
                              Sys.sleep(every)
                              next
                            }
                            queue$clean()
                            return(list(title = msg$title, message = jsonlite::unserializeJSON(msg$message), time = msg$time))
                          } ## while true
                        } ### blockingRead
                      )

)

Minion <- R6Class('Minion',
                  inherit = Despicable,
                  private = list(
                    .workerLibs = c('txtq'),
                    .currentUser = NULL,
                    .process = NULL

                  ),
                  active = list(
                    workerLibs = function(value) {
                      if (missing(value)) {
                        private$.workerLibs
                      } else {
                        private$.workerLibs <- union(value, 'txtq') # make sure we always have the queue lib
                      }
                    }
                  ),

                  public = list(
                    initialize = function(workerLibraries = NULL){
                      self$workerLibs <- workerLibraries
                      private$.process <- parallel::makePSOCKcluster(1)
                      self$loadLibs()
                      self$startQueues()
                    },
                    getUser = function(){
                      private$.currentUser
                    },
                    loadLibs = function(){
                      l <- function(x){
                        lapply(x, library, character.only = TRUE)
                      }
                      parallel::clusterCall(private$.process, l, private$.workerLibs)
                    },
                    bindUser = function(userName, pass, loginDF, timeout = 60){
                      if(is.null(super$reqQ) || is.null(super$resQ)){
                        self$startQueues()
                      }
                      private$.currentUser <- userName
                      minionLogin <- function(usr, pass, logindata){
                        if(exists('opals', envir = .GlobalEnv)){
                          try(datashield.logout(opals))
                        }
                        logindata$user <- usr
                        logindata$password <- pass
                        assign('opals', datashield.login(logins = logindata), envir = .GlobalEnv)
                        ls(envir = .GlobalEnv)
                      }
                      loginMsg <- jsonlite::serializeJSON(list(func = minionLogin, args = list(userName, pass, loginDF)))
                      super$reqQ$push('login', loginMsg)
                      self$blockingRead()
                    },
                    startQueues = function(pollInterval = 1){
                      super$startQueues()

                      listen <- function(poll, reqPath, resPath){  # executed in the cluster node
                        reqQ <- txtq(reqPath)
                        resQ <- txtq(resPath)
                        while(TRUE){
                          msg <- reqQ$pop(1)
                          if(nrow(msg) == 0){
                            Sys.sleep(poll)
                            next
                          }
                          toDo <- jsonlite::unserializeJSON(msg$message)
                          reqQ$clean()
                          if(toDo == 'STOP'){
                            if(exists(opals, envir = .GlobalEnv)){
                              try(datashield.logout(opals))
                              try(rm(opals))
                            }
                            resQ$push('STOP', jsonlite::serializeJSON('STOPPED'))
                            return()
                          }
                          if(is.null(toDo$args)){
                            toDo$args <- list()
                          }

                          tryCatch({
                            res <- do.call(toDo$fun, toDo$args, envir = .GlobalEnv)
                            resQ$push('result', jsonlite::serializeJSON(res))
                          }, error = function(e){
                            msg <- e$message
                            if(grepl('datashield.errors', msg)){
                              msg <- datashield.errors()
                            }
                            resQ$push('error', jsonlite::serializeJSON(msg))
                          })
                        } ## while loop
                      } ## listen
                      # fire and forget :
                      arglist <- list(pollInterval, super$reqQ$path(), super$resQ$path())
                      parallel:::sendCall(private$.process[[1]], listen, arglist)
                    },
                    stopQueues = function(){
                      if(!is.null(private$.currentUser)){
                        self$unbindUser()
                      }
                      super$reqQ$push('STOP', jsonlite::serializeJSON('STOP'))
                      if(super$blockingRead(super$resQ)$message == 'STOPPED'){
                        super$stopQueues()
                      }
                    },
                    unbindUser = function(){
                      disconnect <- function(){
                        datashield.logout(opals)
                        rm(opals)
                        ls()
                      }
                      if(!is.null(super$reqQ)){
                        super$reqQ$push('logout', jsonlite::serializeJSON(list(func = disconnect)))
                      }
                      private$.currentUser <- NULL
                    },
                    finalize = function(){
                      if(!is.null(private$.reqQ)){
                        self$stopQueues()
                      }
                      parallel::stopCluster(private$.process)
                    }

                  )


)

Gru <- R6Class('Gru',
                     inherit = Despicable,
                      private = list(
                        .lReqQ = NULL,
                        .lResQ = NULL
                      ),
                      active = list(
                        lReqQ = function(value) {
                          if (missing(value)) {
                            private$.lReqQ
                          } else {
                            if(!is.null(value) && !any(grepl("R6_txtq", class(value)))){
                              stop('Value must be either a R6_txtq object or NULL.')
                            }
                            if(!is.null(private$reqQ)){
                              private$.lReqQ$destroy
                            }
                            private$.lReqQ <- value
                          }
                        },
                        lResQ = function(value) {
                          if (missing(value)) {
                            private$.lResQ
                          } else {
                            if(!is.null(value) && !any(grepl("R6_txtq", class(value)))){
                              stop('Value must be either a R6_txtq object or NULL.')
                            }
                            if(!is.null(private$resQ)){
                              private$.lResQ$destroy
                            }
                            private$.lResQ <- txtq(value)
                          }
                        }

                      ),

                      public = list(
                      startQueues = function(listenerReqPath = NULL, listenerResPath = NULL, reqPath = NULL, resPath = NULL){
                          if(!is.null(listenerReqPath) && !is.null(listenerResPath)){
                            private$.lReqQ <- txtq(listenerReqPath)
                            private$.lResQ <- txtq(listenerResPath)
                          }
                          if(!is.null(reqPath) && !is.null(resPath)){
                            super$startQueues(reqPath, resPath)
                          }
                        },
                        stopQueues = function(){
                          # not destroying the listener queues
                          private$.lResQ <-NULL
                          private$.lReqQ <-NULL
                          super$stopQueues()
                        },
                        sendRequest = function(func, args = NULL, title = func, queue = super$reqQ){
                          mesg <- jsonlite::serializeJSON(list(fun = func, args = args))
                          queue$push(title, mesg)
                        }
                    )
)





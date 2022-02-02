Minion <- R6Class('Minion',
                      private = list(
                        .currentUser = NULL,
                        .reqQueue = NULL,
                        .resQueue = NULL,
                        .process = NULL

                      ),
                      public = list(
                        getReqQueue = function(){
                          private$.reqQueue
                        },
                        getResQueue = function(){
                          private$.resQueue
                        },
                        initialize = function(){
                           private$.process <- parallel::makePSOCKcluster(1)
                           self$loadLibs()
                           self$startQueues()
                        },
                        loadLibs = function(){
                          l <- function(x){
                            lapply(x, library, character.only = TRUE)
                          }
                          parallel::clusterCall(private$.process, l, c('dsBaseClient', 'dsSwissKnifeClient', 'dsQueryLibrary', 'txtq', 'magrittr'))
                        },
                        bindUser = function(userName, pass, loginDF, timeout = 60){
                          if(is.null(private$.reqQueue) || is.null(private$.resQueue)){
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
                          reqQ <- txtq(private$.reqQueue)
                          reqQ$push('login', loginMsg)
                          self$blockingRead()
                      },
                        startQueues = function(pollInterval = 1){
                          private$.reqQueue <- tempfile(pattern = '', tmpdir = '/tmp')
                          private$.resQueue <- tempfile(pattern = '', tmpdir = '/tmp')

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
                              if(toDo == 'STOP'){
                                reqQ$destroy()
                                resQ$destroy()
                                if(exists(opals, envir = .GlobalEnv)){
                                  try(datashield.logout(opals))
                                }
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
                          arglist <- list(pollInterval, private$.reqQueue, private$.resQueue)
                          parallel:::sendCall(private$.process[[1]], listen, arglist)
                        },
                        stopQueues = function(){
                          reqQ <- txtq(private$.reqQueue)
                          reqQ$push('STOP', jsonlite::serializeJSON('STOP'))
                          private$.reqQueue <- NULL
                          private$.resQueue <- NULL
                        },
                        unbindUser = function(){
                           disconnect <- function(){
                             datashield.logout(opals)
                             rm(opals)
                           }
                           reqQ <- txtq(private$.reqQueue)
                           reqQ$push('logout', jsonlite::serializeJSON(list(func = disconnect)))
                        },
                        blockingRead = function(queue = txtq(private$.resQueue), timeout = 60, every = 1){
                          st <- as.numeric(Sys.time())
                          while(TRUE){
                            msg <- queue$pop(1)
                            if(nrow(msg) == 0){
                              if(as.numeric(Sys.time()) - st > timeout){
                                stop(paste0('Timeout while waiting to read from ', queue$path))
                              }
                              Sys.sleep(every)
                              next
                            }
                            return(jsonlite::unserializeJSON(msg$message))
                          } ## while true
                        } ### blockingRead

                      )
)

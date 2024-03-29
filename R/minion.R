
#'@import digest
#'@export
Minion <- R6::R6Class('Minion',
                      private = list(
                        .reqQ = NULL,
                        .resQ = NULL,
                        .userName = NULL,
                        .nodeResources = NULL
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
                             private$.resQ <- value
                         }
                       },
                       userName = function(value){
                         if(missing(value)){
                           private$.userName
                         } else {
                           if(is.null(value)){
                             stop('userName cannot be null.')
                           }
                         }
                       }
                     ),
                      public = list(
                        getNodeResources = function(){
                          private$.nodeResources
                          },
                        workerLibs = NULL,
                        loadLibs = function(libs = NULL){
                          if(!is.null(libs)){
                            self$workerLibs <- libs
                          }
                          l <- function(x){
                            lapply(x, library, character.only = TRUE)
                          }
                          self$sendRequest(func = l, args = list(self$workerLibs), waitForIt = FALSE)
                        },
                        login = function(pass, wait = FALSE, timeout = 60){
                          if(is.null(self$reqQ) || is.null(self$resQ)){
                            stop('Start the queues first.')
                          }

                          minionLogin <- function(usr, pass, logindata){
                            if(exists('opals', envir = .GlobalEnv)){
                              try(datashield.logout(opals))
                            }
                            logindata$user <- usr
                            logindata$password <- pass
                            assign('opals', datashield.login(logins = logindata), envir = .GlobalEnv)
                            ls(envir = .GlobalEnv)
                          }
                          loginDF <- private$.nodeResources[,!(names(private$.nodeResources) %in% 'node')]
                          self$sendRequest(minionLogin, list(self$userName, pass, loginDF), 'login', waitForIt = wait, timeout = timeout)

                        },
                        stopProc = function(wait = TRUE){
                          self$sendRequest(func = 'STOP', title = 'STOP', waitForIt = wait)
                        },
                        initialize = function(user, logindata, resources, workerLibraries = NULL, queueDir = NULL){
                          private$.userName <- user

                          private$.nodeResources <- lapply(names(resources), function(x){
                            line <- logindata[logindata$server == x,,drop = FALSE]
                            lapply(resources[[x]], function(y){
                              line$node <- line$server # keep the old server name
                              line$server <- y
                              line
                            })
                          }) %>% unlist(recursive = FALSE) %>% Reduce(rbind, .)
                          self$workerLibs <- workerLibraries
                          if(!is.null(queueDir)){
                            self$startQueues(queueDir)
                          }
                        },
                        sendRequest = function(func, args = NULL, title = 'fun', queue = private$.reqQ, waitForIt = TRUE, timeout = 60, every =1){
                          mesg <- jsonlite::serializeJSON(list(fun = func, args = args, waitForIt = waitForIt))
                          queue$clean() #  we clean the previous requests only before a new one, the last one is always there
                          queue$push(title, mesg)
                          if(waitForIt){
                            # yeah, don't think it will reply straight away, start with a break:
                            Sys.sleep(every/2)
                            self$blockingRead(private$.resQ, timeout, every)
                          }
                        },
                        startQueues = function(where = NULL){
                          qPath <- paste0(tempdir(TRUE), '/', where,'/',private$.userName, '_', paste0(runif(1), Sys.time()) %>% digest)
                          if(is.null(private$.userName)){
                            stop('Please set the userName first.')
                          }
                          if(!is.null(self$resQ) && file.exists(self$resQ$path())){
                              warning(paste0('Queue ', resQ$path(), ' exists.'))
                          } else {
                            self$resQ <- txtq( paste0(qPath, '.res'))
                          }
                          if(!is.null(self$reqQ) && file.exists(self$reqQ$path())){

                            warning(paste0('Queue ', rqsQ$path(), ' exists.'))
                          } else {
                            self$reqQ <- txtq(paste0(qPath, '.req'))
                          }
                      },
                        stopQueues = function(){
                          if(!is.null(private$.reqQ)){
                            private$.reqQ$destroy()
                          }
                          if(!is.null(private$.resQ)){
                            private$.resQ$destroy()
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
                            # no response queue cleaning, that's handled by the listener
                            return(list(title = msg$title, message = jsonlite::unserializeJSON(msg$message), time = msg$time))

                          } ## while true
                        } ### blockingRead
                      )

)
#'@export
HeadMinion <- R6::R6Class('HeadMinion',
                          inherit = Minion,
                          private = list(
                            .process = NULL
                          ),
                          public = list(
                            getProc = function(){
                              private$.process
                            },
                            startProc = function(poll = 1, timeout = 1200){
                              if(is.null(super$reqQ) || is.null(super$resQ)){
                                stop('Start the queues first')
                              }
                              reqPath <- super$reqQ$path()
                              resPath <- super$resQ$path()
                              if(!is.null(private$.process)){
                                stop(paste0('Process ', private$.process$get_pid(), ' is already attached.'))
                              }
                              code <- paste0("dsMIP::listen('",reqPath, "','", resPath,"',", poll, ",", timeout, ")")
                              private$.process <- processx::process$new('/usr/bin/Rscript',
                                                                        c('-e',code), cleanup = FALSE, stderr = sub('res', 'err', resPath))

                            },
                            stopProc = function(){
                              result <- super$stopProc()
                              if(grepl('Stopped', result$message)){
                                private$.process <- NULL
                              }
                              invisible(result)
                            },
                            getVars = function(grps,rs){
                              ret <- list(person = c('date_of_birth','gender', 'race','ethnicity'))
                              grps <- setdiff(grps, 'person')
                              gv <- function(groups, res){

                                sapply(groups, function(x){
                                  make.names(ds.levels(paste0(x, '$', x, '_name'), datasources = opals)[[1]]$Levels)

                                }, simplify = FALSE)

                              }

                              realGrps <- self$minionCall(gv, list(grps, rs), async = FALSE)
                              return(realGrps)
                            },

                            reshapeVars = function(vars, moreVars){
                              realGrps <- sapply(vars, function(serverlist){
                                sapply(serverlist, function(srv){
                                  Reduce(union, srv)
                                }, simplify = FALSE) %>% Reduce(union, .)
                              }, simplify = FALSE)
                              c(moreVars, realGrps)
                            }


                          )
)

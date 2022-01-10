#' @import RestRserve
#' @export
WebSession <- R6::R6Class('WebSession',
                   private = list(
                     .sid = NULL, # session id
                     .minion = NULL, # handle to the slave process
                     .lastTime = NULL, # last time it was hit (for timeouts)
                     .minionShop = NULL, # the minion hive object
                     .nodeResources = NULL, # db connection resources on the remote nodes
                     .nodeDFs = NULL # data frames to load on the remote nodes
                   ),
                   public = list(
                     initialize = function(minionShop = NULL, usr, pass, logindata, resources = NULL, groups = NULL){
                         private$.nodeResources <- resources
                         private$.nodeDFs <- groups
                         if(!is.null(minionShop)){ # normal execution
                          private$.minion <- minionShop$provideMinion()
                         } else { #debugging, execute everything in this R session
                          private$.minion <- NULL
                         }
                         private$.minionShop <- minionShop
                         # try to login
                         tryCatch(
                           self$nodeLogin(logindata,usr,pass),
                           error = function(e){
                             private$.minionShop$returnMinion(private$.minion)
                             stop(e$message)
                           })
                         # just for dev:
                         self$minionEval(expression(dssSetOption(list('cdm_schema' = 'synthea_omop'))))
                         self$minionEval(expression(dssSetOption(list('vocabulary_schema' = 'omop_vocabulary'))))
                         ####

                         # load resources:
                         if(!is.null(private$.nodeResources)){
                          self$loadNodeResources()
                         }
                         # and data frames:
                         if(!is.null(private$.nodeDFs)){
                           self$loadNodeData()
                         }

                     },
                     finalize = function(){
                       # if we fail here there's no garbage collection so tryCatch:
                       tryCatch(self$minionEval(expression(datashield.logout(opals))),
                                error = function(e) warning(e$message))
                       #stopCluster(private$.minion) # maybe...
                       # or maybe give it back:
                       if(is.null(private$.minion)){ # debug
                         return()
                       }
                       private$.minionShop$returnMinion(private$.minion)
                     },
                     nodeLogin = function(logindata,usr,pass){
                       minionLogin <- function(logindata,usr, pass){
                         logindata$user <- usr
                         logindata$password <- pass
                         assign('opals', datashield.login(logins = logindata), envir = .GlobalEnv)
                       }

                       self$minionCall(minionLogin, logindata, usr, pass)
                       private$.sid <- paste0(runif(1), Sys.time()) %>% digest
                     },
                     loadNodeResources = function(){
                       remoteLoad <- function(resourceMap){
                        sapply(names(resourceMap), function(x){
                          tryCatch(
                          datashield.assign.resource(opals[x], sub('.','_',resourceMap[[x]], fixed = TRUE), resourceMap[[x]], async = FALSE),
                          error = function(e) stop(datashield.errors())
                          )
                        })
                        datashield.symbols(opals)
                       }
                       self$minionCall(remoteLoad, private$.nodeResources)
                      },
                     loadNodeData = function(){
                       remoteData <- function(dfs){
                         sapply(dfs, function(x){
                          tryCatch(
                           dsqLoad(symbol= x, domain = 'concept_name', query_name = x, datasources = opals),
                           error = function(e) stop(datashield.errors())
                         )
                        })
                         datashield.symbols(opals)
                       }
                       self$minionCall(remoteData, private$.nodeDFs)
                     },
                     minionEval = function(what){
                       private$.lastTime <- Sys.time()
                       if(!is.null(private$.minion)){ # normal execution
                          parallel::clusterCall(private$.minion, eval, what, env = .GlobalEnv)[[1]]
                       } else {
                         eval(what, envir = .GlobalEnv) # debugging
                       }
                     },
                     minionCall = function(fun, ...){
                       private$.lastTime <- Sys.time()
                       if(!is.null(private$.minion)){ # normal execution
                          parallel::clusterCall(private$.minion, fun, ...)[[1]]
                       } else {
                         do.call(fun, list(...))   # debug
                       }
                     },
                     getMinion = function(){
                       private$.minion
                     },
                     getSid = function(){
                       private$.sid
                     },
                     getLastTime = function(){
                       private$.lastTime
                     }
                   )
)
#' @export
MinionHive <- R6Class('MinionHive',
                         private = list(
                           .available = list(), # list of available parallel servers
                           .alumni = 0  # counter of servers in use by sessions
                         ),
                         public = list(
                            getAvaliable = function() private$.available,
                            addMinions = function(how.many = 1){
                              # check the maximum allowed:
                              alive <- length(private$.available) + private$.alumni
                              how.many <- min(how.many, self$maxWorkers - alive)
                              if(how.many > 0){
                                newcl <- parallel::makePSOCKcluster(names = how.many)
                                sapply(self$preloadLibraries, function(x){
                                  tryCatch(
                                  clusterCall(newcl,eval, parse(text = paste0('library(', x,')')),env = .GlobalEnv),
                                  error = function(e) warning(e$message)
                                  )
                                })
                                private$.available<- c(private$.available, newcl)
                                class(private$.available ) <- class(newcl)
                              } else {
                                stop('Server is at maximum capacity.')
                              }
                            },
                            removeMinions = function(how.many = 1){
                              if(how.many <= length(private$.available)){
                                parallel::stopCluster(private$.available[1:how.many])
                                private$.available <- private$.available[how.many +1, length(private$.available)]
                              }
                            },
                            initialize = function(workers = 5, minWorkers = 1, maxWorkers = 20, team = 3, preloadLibraries = NULL){
                                if(workers > maxWorkers){
                                  warning("The value for workers cannot be higher than maxWorkers. Will reduce it to maxWorkers")
                                  workers <- maxWorkers
                                }
                                if(workers < minWorkers){
                                  warning("The value for workers cannot be lower than minWorkers. Will increase it to minWorkers")
                                  workers <- minWorkers
                                }
                                self$minWorkers <- minWorkers
                                self$maxWorkers <- maxWorkers
                                self$team <- team
                                self$preloadLibraries <- preloadLibraries
                                self$addMinions(workers)

                            },
                            adjustAvailable = function(){
                              if(self$team < 0 | (self$maxWorkers - self$minWorkers) < self$team){
                                warning(paste0("Values for minWorkers or maxWorkers or addWorkers or all need adjusting. ",
                                     "Setting them to the default  respective values of 1,20 and 3."))
                                     self$minWorkers <- 1
                                     self$maxWorkers <- 20
                                     self$team <- 3
                              }
                              alive <- length(private$.available) + private$.alumni
                              while(alive < self$minWorkers ){
                                self$addMinions(self$team)
                                alive <- length(private$.available) + private$.alumni
                              }
                              if(alive > self$maxWorkers ){
                                removeMinions(alive - self$maxWorkers)
                              }
                            },
                            provideMinion = function(){
                              if(length(private$.available) == 0){ # only if we have to
                                self$addMinions(self$team) #this fails if we are at capacity
                               }
                              x <- private$.available[1]
                              #remove it from the available:
                              private$.available <- private$.available[2:length(private$.available)]
                              private$.alumni <- private$.alumni + 1

                              #and return it:
                              x
                            },
                            returnMinion = function(minion){
                              # this whole thing is called by the finalize of WebSession so by the gc really
                              private$.available[length(private$.available) +1] <- minion
                              private$.alumni <- private$.alumni - 1
                              self$adjustAvailable()
                            },
                            finalize = function(){
                              parallel::stopCluster(private$.available)
                            },
                            minWorkers = NULL, # minimum workers allowed at any time
                            maxWorkers = NULL, # maximum ....
                            team = NULL,  # how many workers to add at a time
                            preloadLibraries = NULL # vector containing library names to preload at worker creation
                         )
  )


# modified AuthBackendBasic to allow sessions
#' @export
SentryBackend <- R6::R6Class('SentryBackend',
                         inherit = AuthBackendBasic,
                         public = list(
                           authenticate = function(request, response) {
                             mySid <- request$cookies[['sid']][['value']]
                             if (!is.null(mySid)){
                               if(mySid %in% names(sessionList)){ # we do have this session id
                                 response$set_cookie('sid', mySid) # do I set it every time?
                                 return(TRUE)
                               } else { # we don't
                                 stop(e$message, call. = FALSE)
                                 raise(self$HTTPError$unauthorized(
                                   body = "401 Invalid session ID",
                                   headers = list("WWW-Authenticate" = "Basic"))
                                 )
                               }

                             }
                             # if we are here we have no session id, check the credentials:
                             user_password = private$extract_credentials(request, response)
                             newSession <-tryCatch(
                                        private$auth_fun(user_password[[1]], user_password[[2]]),
                                        error = function(e){
                                          stop(e$message, call. = FALSE)
                                          raise(self$HTTPError$unauthorized(
                                            body = paste(e," --- 401 Invalid Username/Password"),
                                            headers = list("WWW-Authenticate" = "Basic"))
                                          )
                                        })
                             # if no error above, we have a new sid, let everybody know:
                             mySid <- newSession$getSid()
                             sessionList[[mySid]] <<- newSession
                             cookies <- request$cookies
                             cookies$sid <- list(name = 'sid', value= mySid)
                             request$cookies <- cookies
                             response$set_cookie('sid', mySid)
                           } # authenticate

                         )
)

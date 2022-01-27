#' @import RestRserve, digest, dsSwissKnifeClient, dsQueryLibrary, dsBaseclient
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
                         self$minionEval(expression(dssSetOption(list('cdm_schema' = 'synthea_omop'))), async = TRUE)
                         self$minionEval( expression(dssSetOption(list('vocabulary_schema' = 'omop_vocabulary'))), async = TRUE)
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
                       tryCatch(self$minionEval(async = FALSE, expression(datashield.logout(opals))),
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
                         print('wait')
                         assign('opals', datashield.login(logins = logindata), envir = .GlobalEnv)
                         return(NULL)
                       }

                       self$minionCall( minionLogin, list(logindata, usr, pass), async = TRUE)
                       private$.sid <- paste0(runif(1), Sys.time()) %>% digest
                     },
                     loadNodeResources = function(){
                       remoteLoad <- function(resourceMap){
                        sapply(names(resourceMap), function(x){

                          tryCatch(
                          sapply(resourceMap[[x]], function(y){

                            datashield.assign.resource(opals[x], sub('.','_',y, fixed = TRUE), y, async = FALSE)
                            }),
                          error = function(e) stop(datashield.errors())
                          )
                        })
                      #  datashield.symbols(opals)
                        return(NULL)
                       }
                       self$minionCall(remoteLoad, list(private$.nodeResources), async = TRUE)

                      },
                     loadNodeData = function(){
                       remoteData <- function(dfs){

                         sapply(dfs, function(x){
                          tryCatch(
                           dsqLoad(symbol= x, domain = 'concept_name', query_name = x, union = FALSE, datasources = opals),
                           error = function(e) stop(datashield.errors())
                         )
                        })
                         # datashield.symbols(opals)
                         return(NULL)
                       }
                       self$minionCall(remoteData, list(private$.nodeDFs), async = TRUE)
                     },
                     minionEval = function(what, async = FALSE, tag = NULL, timeout = 60){
                    #   private$.lastTime <- Sys.time()
                       if(!is.null(private$.minion)){ # normal execution
                    #      parallel::clusterCall(private$.minion, eval, what, env = .GlobalEnv)[[1]]
                          self$minionCall(fun = eval, list(what, env = .GlobalEnv), async = async, tag = tag, timeout = timeout)
                       } else {
                         private$.lastTime <- Sys.time()
                         eval(what, envir = .GlobalEnv) # debugging
                       }
                     },
                     minionCall = function(fun, arglist = list(), async = FALSE, tag = NULL, timeout = 60){
                       # now with improved async functionality
                       private$.lastTime <- Sys.time()
                       if(is.null(tag)){
                         tag <- as.numeric(Sys.time())
                       }

                      if(!is.null(private$.minion)){ # normal invocation
                        parallel:::sendCall(private$.minion[[1]], fun, arglist, tag  = tag)
                        if(async){
                          return(tag)
                        } else { # sync

                            self$getResult(tag, timeout = timeout)[[1]]

                        }
                      } else {
                           do.call(fun, arglist)   # debug
                      }

                     },
                     getResult = function(tag, timeout = 60){
                       st <- as.numeric(Sys.time()) # start time

                       oldTimeout <- socketTimeout(private$.minion[[1]]$con, 1) # poll for one second
                       while(TRUE){
                        ret <- try(unserialize(private$.minion[[1]]$con), silent = TRUE)
                        if('tag' %in% names(ret) && ret$tag == tag){ # wait for the result with my tag (discard all the others)
                          socketTimeout(private$.minion[[1]]$con, oldTimeout)
                          return(parallel:::checkForRemoteErrors(list(ret$value)))
                        }

                         if(as.numeric(Sys.time()) - st > timeout){
                           # reset the socket timeout
                           socketTimeout(private$.minion[[1]]$con, oldTimeout)
                           stop(paste0('Reached timeout of ', timeout, ' seconds while waiting for results with tag ', tag))
                         }
                       }
                       # reset the socket timeout
                       socketTimeout(private$.minion[[1]]$con, oldTimeout)
                       ret$value
                     },
                     getMinion = function(){
                       private$.minion
                     },
                     getSid = function(){
                       private$.sid
                     },
                     getLastTime = function(){
                       private$.lastTime
                     },
                    getVars = function(){
                      grps <- private$.nodeDFs
                      rs <- private$.nodeResources
                      ret <- list(person = c('date_of_birth','gender', 'race','ethnicity'))
                      grps <- setdiff(grps, 'person')
                      gv <- function(groups, res){

                           sapply(groups, function(x){
                             sapply(names(res), function(node){
                            suffixes <- sub('.', '_', res[[node]], fixed = TRUE)
                             objs <- paste(x, suffixes, sep = '_')

                              sapply(objs, function(z){
                                ds.levels(paste0(z, '$', x, '_name'), datasources = opals[node])[[1]]$Levels

                              }, simplify = FALSE)

                          }, simplify = FALSE)
                        }, simplify = FALSE)
                      }
                      realGrps <- self$minionCall(gv, list(grps, rs), async = FALSE)
                      realGrps <- sapply(realGrps, function(serverlist){
                        sapply(serverlist, function(srv){
                          Reduce(union, srv)
                        }, simplify = FALSE) %>% Reduce(union, .)
                      }, simplify = FALSE)
                      c(ret, realGrps)
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
                                          stop(e$message)
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

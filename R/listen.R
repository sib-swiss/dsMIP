#' @export

listen <- function( usr, pwd, logindata, resourceMap, reqPath, sourceFile, libs = NULL,  every = 1, heartbeatInterval = 300){
  #there will be one or more dameons running each a copy of this function, all logged into the remote nodes, all servicing one request queue
  # sourceFile contains all the functions that will be invoked by the endpoints
  source(sourceFile)
  reqQ <- txtq::txtq(reqPath) # the requests queue

  opals <- .login(usr, pwd, logindata, libs)
  varmap <- .load(opals, resourceMap)

  ################## round and round #########################################
  st <- as.numeric(Sys.time())
  while(TRUE){
    msg <- reqQ$pop(1)

    if(nrow(msg) == 0){
      nw <- as.numeric(Sys.time())
      if( nw - st > heartbeatInterval){
        # ping everybody to avoid timeouts:
        datashield.symbols(opals)
        # reset the timer:
        st <- nw
      } else { # the above takes  time, only sleep if necessary:
        Sys.sleep(every)
      }
      next
    }
    ###### from here on there's business ####################################
    toDo <- jsonlite::unserializeJSON(msg$message)
    #### the message has this structure:
    # $resPath - path to the response queue
    # $pid - pid of the requesting process (to differentiate between 2 simultaneous requests from the same user)
    # $fun - the *name* of the function to be called (the function must be defined before)
    # $args - a list containing the function arguments
    # $waitForIt -  a flag indicating a result should be returned in the response queue
    ############
    if(is.null(toDo$waitForIt)){
      toDo$waitForit <- FALSE
    }
    if(is.null(toDo$resPath)){
     if(toDo$waitForIt){
        warning('No response queue defined, not executing.')
        next
     }
    }
    if(!file.exists(toDo$resPath)) {
      file.create(toDo$resPath)
    }
    # ok we can use the queue:
    resQ <- txtq(toDo$resPath)
    if(!is.character(toDo$fun)){
      resQ$push('error', jsonlite::serializeJSON(list(pid = toDo$pid, message = '"Fun" must me a function name, a character.')))
      next
    }

    ####### deal with the"stop" message ######

    if(toDo$fun == 'STOP'){
      stopMessage <- 'Stopped'
      tryCatch({datashield.logout(opals)
        stopMessage <- paste0(stopMessage, ' and logged out.')},
        error = function(e){
          resQ$push('error', jsonlite::serializeJSON(list(pid = toDo$pid, message = e$msg)))
        })
      resQ$push('STOP', jsonlite::serializeJSON(list(pid = toDo$pid, message = stopMessage)))
      return()
    }

    ########################

    activeFunc <- .GlobalEnv[[toDo$fun]]
    if(is.null(activeFunc)){
      source(sourceFile) # try again
      if(is.null(activeFunc)){
        resQ$push('error', jsonlite::serializeJSON(list(pid = toDo$pid, message = 'Function not found')))
        next
      }
    }

    if(is.null(toDo$args)){
      toDo$args <- list()
    }

    resQ$clean() # before sending the response, like that the last response is always in the queue for later inspection
    tryCatch({
      res <- do.call(activeFunc, toDo$args)
      if(toDo$waitForIt){
        title <- 'result'
        if(msg$title != 'fun'){
          title <- msg$title
        }
        resQ$push(title, jsonlite::serializeJSON(res))
      }
    }, error = function(e){
      res <- e$message
      if(grepl('datashield.errors', res)){ # that's an error on the node(s)
        res <- datashield.errors()
      }
      resQ$push('error', jsonlite::serializeJSON(list(pid = toDo$pid, message = res)))
    }, finally = {
      st <- as.numeric(Sys.time())    # either way reset the timer
    })
  } ## while loop
}

.login <- function(usr, pwd, libs = NULL, logindata, resourceMap){
  libs <- unique(c(libs, 'dsSwissKnifeClient', 'dsQueryLibrary', 'jsonlite' , 'magrittr'))
  lapply(libs, library, character.only = TRUE) # load the libraries
  # finalise logindata
  logindata$user <- usr
  logindata$password <- pwd
  logindata$driver <- 'OpalDriver'

  ######### make one logindata entry per resource (as opposed to one per server - we'll have one or more connections per server) ###########
  resnames <- dssSwapKeys(resourceMap)
  logindata <- lapply(names(resnames), function(x){
    out <- logindata[logindata$server == resnames[[x]],,drop=FALSE]
    out$server <- x
    out
  }) %>% Reduce(rbind,.)
  ##################################################
  ######################### login where allowed, fail silently elsewhere #################################
  opals <- list()
  for(i in logindata$server){
    try(opals[i] <- datashield.login(logindata[logindata$server == i,,drop = FALSE]), silent = TRUE)
  }
  return(opals)
}


# the heavy load:
.load <- function(datasources, resourceMap){
  #first the resources:
  sapply(names(resourceMap), function(res){
    datashield.assign.resource(opals[res], sub('.','_',resourceMap[[res]], fixed = TRUE), resourceMap[[res]], async = FALSE)
  })
  # load the 2 data frames
  dsqLoad(symbol= 'measurement',
          domain = 'concept_name',
          query_name = 'measurement',
          where_clause = 'value as number is not null',
          row_limit =  3000000, ## tayside doesn't handle more
          union = TRUE,
          datasources = datasources)
  dsqLoad(symbol= 'person',
          domain = 'concept_name',
          query_name = 'person',
          where_clause = 'value as number is not null',
          union = TRUE,
          datasources = datasources)
  # fix funky measurement dates:
  dssSubset('measurement', 'measurement', row.filter = 'measurement_date >= "01-01-1970', datasources = datasources)

  ############## calculate age #####################

  # order by measurement date for each person_id
  dssSubset('measurement', 'measurement', 'order(person_id, measurement_date)', async = TRUE, datasources = datasources)
  #  measurement dates as numbers:
  dssDeriveColumn('measurement', 'measurement_date_n', 'as.numeric(as.Date(measurement_date))', datasources = datasources)
  # add a dummy column just for the widening formula, this will hold eventually the 'aggregate' first measurement date
  dssDeriveColumn('measurement', 'f', '"irst_measurement_dat.e"', datasources = datasources)
  # now we can widen by that column and pick the first value:
  dssPivot(symbol = 'first_m_dates', what ='measurement', value.var = 'measurement_date_n',
           formula = 'person_id ~ f',
           by.col = 'person_id',
           fun.aggregate = function(x)x[1], # we are sure it's the first date, baseline, they've been ordered
           async = TRUE,
           datasources = datasources)

  dssJoin(c('person', 'first_m_dates'), symbol= 'person', by = 'person_id', join.type = 'inner', datasources = datasources)
  try(datashield.rm(datasources, 'first_m_dates'), silent = TRUE) # keep it slim
  # now calculate the age at first measurement:
  dssDeriveColumn('person', 'age', 'round((f.irst_measurement_dat.e - as.numeric(as.Date(birth_datetime)))/365)', datasources = datasources)

  ###################  finished with age ##########################################

  dssPivot(symbol = 'wide_m', what ='measurement', value.var = 'value_as_number',
           formula = 'person_id ~ measurement_name',
           by.col = 'person_id',
           fun.aggregate = function(x)x[1], # maybe we'll want mean here?
           datasources = datasources)
  try(datashield.rm(datasources, 'measurement'), silent = TRUE)
  dssJoin(what = c('wide_m', 'person'),
          symbol = 'working_set',
          by = 'person_id',
          datasources = datasources)
  dssSubset('working_set', 'working_set', col.filter = 'setdiff(colnames(working_set), c("database", "f.irst_measurement_dat.e", "birth_datetime" , "location_id", "provider_id" , "care_site_id", "person_id")) ') # get rid of superfluous columns
  try(datashield.rm(datasources, 'person'), silent = TRUE)
  try(datashield.rm(datasources, 'wide_m'), silent = TRUE)
  #### fix column names:
  n <- dssColNames('working_set', datasources = datasources)
  sapply(names(n), function(x){
    cnames <- n[[x]]
    cnames <- sub('measurement_name.', '', cnames, fixed = TRUE)
    dssColNames('working_set', cnames, datasources = datasources[[x]])
  })
 # create varmap:
  varsToCohorts <- dssSwapKeys(n)
  varsToCohorts <- sapply(varsToCohorts, function(a){
    list(type = 'number', cohorts = a)
  }, simplify = FALSE)
  varsToCohorts[c('ethnicity', 'race', 'gender')] <- sapply(varsToCohorts[c('ethnicity', 'race', 'gender')], function(a){
                                                              list(type = 'nominal', cohorts = a$cohorts)
                                                     }, simplify = FALSE)
return(varsToCohorts)
}






listen <- function(reqPath,  every = 1, timeout = 1200){  # executed in the cluster node
  reqQ <- txtq::txtq(reqPath)
  resQ <- txtq::txtq(resPath)
  st <- as.numeric(Sys.time())
  while(TRUE){
    msg <- reqQ$pop(1)

    if(nrow(msg) == 0){
      ## first deal with a possible timeout:
      if(as.numeric(Sys.time()) - st > timeout){
        errmsg <- paste0("Timeout of ", timeout, " seconds has been reached.")
        if(exists('opals', envir = .GlobalEnv)){  # be nice, logout first
          tryCatch(datashield.logout(opals),
                   error = function(e){
                     errmsg <<- paste0(errmsg, " Additionally: ", e$msg)
                  })
        }
        resQ$push('timeout', jsonlite::serializeJSON(errmsg))
        stop(errmsg) # timeout
      } # end timeout
      Sys.sleep(every)
      next
    }

    toDo <- jsonlite::unserializeJSON(msg$message)
    # no reqQ cleaning, that's handled by the minion at the next request
    if(is.character(toDo$fun) && toDo$fun == 'STOP'){
      stopMessage <- 'Stopped'
      if(exists('opals', envir = .GlobalEnv)){
        tryCatch({datashield.logout(opals)
                  stopMessage <- paste0(stopMessage, ' and logged out.')},
                 error = function(e){
                   resQ$push('error', jsonlite::serializeJSON(e$msg))
                 })
      }
      resQ$push('STOP', jsonlite::serializeJSON(stopMessage))
      return()
    }
    if(is.null(toDo$args)){
      toDo$args <- list()
    }
    resQ$clean() # before sending the response, like that the last response is always in the queue for later inspection
    tryCatch({
      res <- do.call(toDo$fun, toDo$args, envir = .GlobalEnv)
      if(!is.null(toDo$waitForIt) && toDo$waitForIt){
        title <- 'result'
        if(msg$title != 'fun'){
          title <- msg$title
        }
        resQ$push(title, jsonlite::serializeJSON(res))
      }
    }, error = function(e){
      res <- e$message
      if(grepl('datashield.errors', res)){
        res <- datashield.errors()
      }
      resQ$push('error', jsonlite::serializeJSON(res))
    }, finally = {
      st <- as.numeric(Sys.time())    # either way reset the timer
    })
  } ## while loop
} ## listen




#' @export

#listen <- function(confFile, reqPath, sourceFile,  every = 1, heartbeatInterval = 300){
  listen <- function(confFile, reqPath, every = 1, heartbeatInterval = 300){
  #there will be one or more dameons running each a copy of this function, all logged into the remote nodes, all servicing one request queue
  # sourceFile contains all the functions that will be invoked by the endpoints
  library(txtq)
  library(jsonlite)
  library(magrittr)

  reqQ <- txtq(reqPath) # the requests queue

  .processConf(confFile)
  .sourceFuncs(config$listenerFuncDir)
  sapply(config$listenerStartupFuncs, function(x) do.call(x, list(), envir = .GlobalEnv)) # execute the startup functions (login, prepare data, etc)

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
    #### no pid for now, try with one response queue per request # $pid - pid of the requesting process (to differentiate between 2 simultaneous requests from the same user)
    # $fun - the *name* of the function to be called (the function must be defined before)
    # $args - a list containing the function arguments
    # $waitForIt -  a flag indicating a result should be returned in the response queue
    ############
    if(is.null(toDo$waitForIt)){
      if(!is.null(toDo$resPath)){
        toDo$waitForIt <- TRUE
      } else {
        toDo$waitForIt <- FALSE
      }
    }
    if(is.null(toDo$resPath)){ # first we need to be able to send responses
      if(toDo$waitForIt){
        warning('No response queue defined, not executing.')
        next
      }
    }
    if(!is.null(toDo$resPath)){
    # ok we can use the queue:
      resQ <- txtq(toDo$resPath)
    }
    ####### deal with the"stop" message ######

    if(msg$title == 'STOP'){
      stopMessage <- 'Stopped'
      tryCatch({datashield.logout(opals)
        stopMessage <- paste0(stopMessage, ' and logged out.')},
        error = function(e){
        if(exists('resQ')) resQ$push('error', jsonlite::toJSON(e$msg))
        })
      if(exists('resQ')){
        resQ$push('STOP', jsonlite::toJSON(stopMessage))
      }
      return()
    }

    ########################



    if(is.null(toDo$fun)){
      if(exists('resQ')) resQ$push('error', jsonlite::toJSON( list(message = 'Nothing to do.', toDo = toDo)))
    }


#######extra security, to be enabled later: ##############
    if(!is.character(toDo$fun)){
      resQ$push('error', jsonlite::serializeJSON(list(pid = toDo$pid, message = '"fun" must me a function name, a character.')))
      next
    }
#####################

    activeFunc <- NULL
    try( activeFunc <- get(toDo$fun))
    if(is.null(activeFunc)){
      .sourceFuncs(config$listenerFuncDir) # try again
      try( activeFunc <- get(toDo$fun))
      if(is.null(activeFunc)){
        if(exists('resQ')) resQ$push('error', jsonlite::toJSON( list(message = 'Function not found')))
        next
      }
    }


    if(is.null(toDo$args)){
      toDo$args <- list()
    }

    if(exists('resQ')) resQ$clean() # before sending the response, like that the last response is always in the queue for later inspection

    tryCatch({
      res <- do.call(activeFunc, toDo$args)
      if(toDo$waitForIt){
        title <- 'result'
        if(msg$title != 'fun'){
          title <- msg$title
        }
        resQ$push(title, jsonlite::toJSON(res, auto_unbox = TRUE))
      }
    }, error = function(e){
      res <- e$message
      if(grepl('datashield.errors', res)){ # that's an error on the node(s)
        res <- datashield.errors()
      }
      resQ$push('error', jsonlite::toJSON(list(res), auto_unbox = TRUE))
    }, finally = {
      st <- as.numeric(Sys.time())    # either way reset the timer
    })
  } ## while loop
}

.login <- function(usr, pwd, libs = NULL, logindata, resourceMap){
  libs <- unique(c(libs, 'dsSwissKnifeClient', 'dsQueryLibrary'))
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
    try(opals[i] <- datashield.login(logindata[logindata$server == i,,drop = FALSE]), silent = FALSE)
    #opals[i] <- datashield.login(logindata[logindata$server == i,,drop = FALSE])
  }
  return(opals)
}


# the heavy load:
.load <- function(datasources){
  #first the resources:
  sapply(names(datasources), function(res){
    datashield.assign.resource(datasources[res], sub('.','_',res, fixed = TRUE), res, async = FALSE)
  })
  # load the 2 data frames
################ !!!!!!!!!!!!!!!!!!!!!! ############### only for development!!!!!
  dssSetOption(list('cdm_schema' = 'synthea_omop'), datasources = datasources)
  dssSetOption(list('vocabulary_schema' = 'omop_vocabulary'), datasources = datasources)
##########################################
  tryCatch(dsqLoad(symbol= 'measurement',
          domain = 'concept_name',
          query_name = 'measurement',
          where_clause = 'value_as_number is not null',
        #  row_limit =  3000000, ## tayside doesn't handle more
          row_limit =  300, ## dev only
          union = TRUE,
          datasources = datasources), error = function(e){
            stop(datashield.errors())
          })
  dsqLoad(symbol= 'person',
          domain = 'concept_name',
          query_name = 'person',
          union = TRUE,
          datasources = datasources)
  ################ !!!!!!!!!!!!!!!!!!!!!! ############### only for development!!!!!
  dssDeriveColumn('measurement', 'measurement_date', '"12-11-2005"', datasources = datasources)
  ##########################################


  # fix funky measurement dates:

  dssSubset('measurement', 'measurement', row.filter = 'measurement_date >= "01-01-1970"', datasources = datasources)

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
  dssSubset('working_set', 'working_set', col.filter = 'setdiff(colnames(working_set), c("database", "f.irst_measurement_dat.e", "birth_datetime" , "location_id", "provider_id" , "care_site_id", "person_id")) ', datasources = datasources) # get rid of superfluous columns
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


.processConf <- function(confFile){
  config <- readChar(confFile, file.info(confFile)$size) %>%
    gsub('(?<=:)\\s+|(?<=\\{)\\s+|(?<=,)\\s+|(?<=\\[)\\s+','',., perl = TRUE) %>%
    fromJSON()
   assign('config', config, envir = .GlobalEnv)
   libs <- unique(c(config$libraries, 'dsSwissKnifeClient', 'dsQueryLibrary'))
   lapply(libs, library, character.only = TRUE) # load the libraries

}

.sourceFuncs <- function(srcDir){
  sapply(list.files(srcDir, full.names = TRUE), source)
}


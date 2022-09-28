#'@export
listen <- function(confFile, reqPath, every = 1, heartbeatInterval = 300){
  #there will be one or more dameons running each a copy of this function, all logged into the remote nodes, all servicing one request queue
  # sourceFile contains all the functions that will be invoked by the endpoints
  library(txtq)
  library(jsonlite)
  library(magrittr)

  reqQ <- txtq(reqPath) # the requests queue
  # obligatory startup processing:
  .processConf(confFile)
  .sourceFuncs(config$listenerFuncDir)
  # whatever the app wants to add here:
  sapply(config$listenerStartupFuncs, function(x){
    tryCatch(do.call(x, list(), envir = .GlobalEnv),
             error = function(e){
               if(grepl('datashield.errors', e)){ # that's an error on the node(s)
                 e <- datashield.errors()
               }
                 stop(e)
               })

    }) # execute the startup functions (*login*, prepare data, etc)

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



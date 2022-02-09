#' @export
listen <- function(reqPath, resPath, every = 1, timeout = 1200){  # executed in the cluster node
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
      }
      Sys.sleep(every)
      next
    }
    toDo <- jsonlite::unserializeJSON(msg$message)
    reqQ$clean()
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
    resQ$clean() # before sending the response, like that the last response is always in the queue for inspecion by the reaper
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
    })
  } ## while loop
} ## listen

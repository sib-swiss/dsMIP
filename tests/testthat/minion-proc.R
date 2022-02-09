

listen <- function(poll, paths){  # executed in the cluster node
  reqQ <- txtq::txtq(paths[1])
  resQ <- txtq::txtq(paths[2])
  while(TRUE){
    msg <- reqQ$pop(1)
    if(nrow(msg) == 0){
      Sys.sleep(poll)
      next
    }
    toDo <- jsonlite::unserializeJSON(msg$message)
    reqQ$clean()
    if(toDo == 'STOP'){
      if(exists('opals', envir = .GlobalEnv)){
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
      if(!is.null(toDo$expectResult)  !todo$expectResult){
        resQ$push('result', jsonlite::serializeJSON(res))
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

invisible(listen(1, commandArgs(trailingOnly = TRUE)))




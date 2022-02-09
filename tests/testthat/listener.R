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

Rscript --vanilla --quiet  /mnt/shareddisk/datashield/dsMIP/tests/testthat/minion-proc.R /tmp/a /tmp/b
p <- process$new('/usr/bin/Rscript',
                 c('--vanilla', '--quiet',
                   '/mnt/shareddisk/datashield/dsMIP/tests/testthat/minion-proc.R',
                   '/tmp/a', '/tmp/b', 'cleanup = FALSE', stderr = '/tmp/processx')
)

req <- txtq::txtq('/tmp/a')
res <- txtq::txtq('/tmp/b')

req$push('STOP', jsonlite::serializeJSON('STOP'))
res$pop()
req$pop()
req$clean()
res$clean()

req$push('pid', jsonlite::serializeJSON(list(func = Sys.getpid)))

addMinions <- function(howmany, threshold, libs = NULL, horde = list()){
  for(i in 1:howmany){
    l <- length(horde)
    if(l >= threshold){
      warning(paste0("Reached the maxWorkers limit of ", threshold))
      break
    }
    horde[[l+1]] <- Minion$new(libs)
  }
  return(horde)
}

getKevin <- function(horde){
  # just scan it, it's small:
  l <- length(horde)
  for(i in 1:l){
    if(is.null(horde[[i]]$getUser())){ # no prior commitments
      return(horde[[i]])
    }
  }
}

# main


minionHorde <- addMinions(config$workers, config$maxWorkers, config$libraries)
kev <- getKevin(minionHorde)

length(minionHorde)
minionHorde <- addMinions(config$addWorkers, config$maxWorkers, config$libraries, minionHorde)

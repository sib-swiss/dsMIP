library(jsonlite)
library(magrittr)

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

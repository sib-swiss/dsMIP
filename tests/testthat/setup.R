library(jsonlite)
library(magrittr)


stopThem <<- TRUE
dir.create(paste0(tempdir(check = TRUE), '/cache'))
x <- system2('docker', args = c('ps'), stdout = TRUE )
if(length(grep('docker_nodes_mip',x)) < 6){
  system2('docker-compose', args = c('-f', '../docker_nodes_mip/docker-compose.yml', 'up', '-d'))
  Sys.sleep(60)
  x <- system2('docker', args = c('ps'), stdout = TRUE )
  stopThem <<- TRUE
}
confFile <- '../config2.json'
config <- readChar(confFile, file.info(confFile)$size) %>%
  gsub('(?<=:)\\s+|(?<=\\{)\\s+|(?<=,)\\s+|(?<=\\[)\\s+','',., perl = TRUE) %>%
  fromJSON()
if(stopThem){
  conts <<- lapply(x, function(y){
    out <- strsplit(y, '\\s+')[[1]]
    out[length(out)]
  }) %>% unlist
  conts[1] <- 'stop' # conts now contains the args for a 'docker stop' command - will be executed in teardown
}


# launch the listener(s)
###
Sys.setenv(pass = 'guest123')
genPath <- paste0(tempdir(TRUE), '/', config$dir)
dir.create(genPath)
reqPath <- paste0(genPath, '/requests')
reqQ <- txtq(reqPath)
sourceFile <- '../listener_functions.R'
listeners <- list()
for(i in 1:config$workers){
  Sys.sleep(20)
    outPath <- paste0(genPath, '/out_', i)
  errPath <- paste0(genPath, '/err_', i)

  code <- paste0("dsMIP::listen('",confFile, "','", reqPath, "','", sourceFile,"')")
  print(code)
  listeners[[i]] <- processx::process$new('/usr/bin/Rscript',
                                          c('-e',code), cleanup = FALSE, stderr = errPath, stdout = outPath)

}
###

sentry <- function(user , password = NULL, sid = NULL ){ # must return a sid
  pipeDir <- paste0(tempdir(TRUE), '/',config$dir)

  if(is.null(sid)){ # we must login
    if(is.null(password)){ # don't even
      return(NULL)
    }
    newSid <- paste0(runif(1), Sys.time()) %>% digest
    newPath <- paste0(pipeDir, '/', user,'_', newSid, '/1') # new pipes in here starting with 1
    myQ <- lckq(paste0(newPath))
    # send the login command to the listener(s)

    mesg <- list(fun = 'authLogin', args = list(user, password), resPath = newPath)

  }


  carl <- HeadMinion$new(user, config$loginData, config$resourceMap,  config$libraries)
  carl$startQueues(config$dir)
  carl$startProc()
  carl$loadLibs()
  logged <- carl$login(password, TRUE)

  if(logged$title == 'error'){
    stop(logged$message)
  }
  devSetOptions <- function(){ # only in dev
    dssSetOption(list('cdm_schema' = 'synthea_omop'))
    dssSetOption(list('vocabulary_schema' = 'omop_vocabulary'))
  }
  carl$sendRequest(devSetOptions, waitForIt = FALSE)

  return(carl)
}

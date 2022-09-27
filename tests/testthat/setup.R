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
resPath <- paste0(genPath, '/responses')
reqQ <- txtq(reqPath)

listeners <- list()
for(i in 1:config$workers){
  Sys.sleep(3)
  outPath <- paste0(genPath, '/out_', i)
  errPath <- paste0(genPath, '/err_', i)
  code <- paste0("dsMIP::listen('",confFile, "','", reqPath, "')")
  print(code)
  listeners[[i]] <- processx::process$new('/usr/bin/Rscript',
                                          c('-e',code), cleanup = FALSE, stderr = errPath, stdout = outPath)

}
#########
## app stuff:

sentry <- makeSentryFunction(requestQ = reqQ, responsePath = resPath)

sentryBackend <- SentryBackend$new( FUN = sentry)

sentryMw <- AuthMiddleware$new(
  auth_backend = sentryBackend,
  routes = "/",
  match = "partial",
  id = "sentry_middleware"
)

app <-  Application$new(content_type = "application/json", middleware = list(sentryMw))

do.call

sapply(list.files(config$listenerFuncDir, full.names = TRUE), function(x) source(x, local = lst))

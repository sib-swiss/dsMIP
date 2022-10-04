library(jsonlite)
library(magrittr)

stopThem <<- TRUE
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
Sys.sleep(5)
for(i in 1:config$workers){

  outPath <- paste0(genPath, '/out_', i)
  errPath <- paste0(genPath, '/err_', i)
  code <- paste0("dsMIP::listen('",confFile, "','", reqPath, "')")
  print(code)
  listeners[[i]] <- processx::process$new('/usr/bin/Rscript',
                                          c('-e',code), cleanup = TRUE, stderr = errPath, stdout = outPath)
  Sys.sleep(5)
}
# get the varmap:

#varmap <-qCommand(reqQ, resPath, message = list(fun = 'identity', args = list(quote(varmap))))$message %>% fromJSON()
#cnames <-qCommand(reqQ, resPath, message = list(fun = 'identity', args = list(quote(cnames))))$message %>% fromJSON()
globalResPath <- paste0(resPath, '/global')
bubbleJSON <-qCommand(reqQ, globalResPath, message = list(fun = 'identity', args = list(quote(bubble))))$message ### %>% fromJSON(simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
#########

#### function to reload the app:

loadApp <- function(middleW){
  app <-  Application$new(content_type = "application/json", middleware = list(middleW))

  # load the enpoint descriptions in a sandbox env:
  temp <- new.env()
  for(p in list.files(config$endpointDir, full.names = TRUE)){
    source(p, local = temp)
  }

  # and attach them to the app (name of endpoint in the file must match the path)
  for(endPath in ls(temp)){
    endp <- get(endPath, temp)
    endp$path <- paste0('/', endPath)
    do.call(app$add_route, endp)
  }
  rm(temp)
  return(app)
}


##### function to reload the listener funcs =====

reloadListener <- function(){
  qCommand(reqQ, resPath, message = list(fun = '.sourceFuncs', args = list(config$listenerFuncDir)))
}

#####
deleteme <- function(){
  qCommand(reqQ, resPath, message = list(fun = 'datashield.symbols', args = list(quote(opals))))$message %>% fromJSON()
  qCommand(reqQ, resPath, message = list(fun = 'ds.table', args = list("working_set$race", datasources = quote(opals['sophia.db']))))
  qCommand(reqQ, globalResPath, message = list(fun = 'datashield.aggregate', args = list(quote(opals), quote(quote(selfUpgrade('dsBase' ,NULL, NULL, NULL , NULL, TRUE))))))
  qCommand(reqQ, resPath, title = 'STOP')

  }

## app stuff:

sentry <- makeSentryFunction(requestQ = reqQ, responsePath = resPath)

sbck<- SentryBackend$new( FUN = sentry)

sentryMw <- AuthMiddleware$new(
  auth_backend = sbck,
  routes = "/",
  match = "partial",
  id = "sentry_middleware"
)

app <- loadApp(sentryMw)

#app <-  Application$new(content_type = "application/json", middleware = list(sentryMw))


#for(p in list.files(config$endpointDir, full.names = TRUE)){
#  temp <- new.env()
#  source(p, local = temp)
 # do.call(app$add_route, as.list(temp)[[1]])
#}

# load the enpoint descriptions in a sandbox env:
#temp <- new.env()
#for(p in list.files(config$endpointDir, full.names = TRUE)){
 # source(p, local = temp)
#}

# and attach them to the app (name of endpoint in the file must match the path)
#for(endPath in ls(temp)){
#  endp <- get(endPath, temp)
#  endp$path <- paste0('/', endPath)
#  do.call(app$add_route, endp)
#}
#rm(temp)

#xxx <- lapply(names(varmap), function(x){
#  thisone <- varmap[[x]]
#  out <- thisone[setdiff(names(thisone), 'cohorts')]
#  out$id <- x
#  out
#})

#enum <- varmap$ethnicity$enumerations %>% Reduce(union, .) %>% lapply(function(en) list(label = en, value = en))
#varsToCohorts[c('ethnicity', 'race', 'gender')] <- sapply(c('ethnicity', 'race', 'gender'), function(a){
#  enum <- c('FEMALE', 'MALE')%>% lapply(function(en)list(label = en, value = en)) # maybe we'll want per cohort at some point?
#  list(type = 'nominal', cohorts = varsToCohorts[[a]]$cohorts, enumerations = enum)
#}, simplify = FALSE)


#bb <- lapply(names(varsToCohorts), function(x){
#  thisone <- varsToCohorts[[x]]
#  out <- thisone[setdiff(names(thisone), 'cohorts')]
#  out$id <- x
#  out
#}
#load('/mnt/shareddisk/mip/datashield-engine/vars.RData')
#which(!mapply(function(x,y){
##str(x)
 # str(y[names(x)])
#  if (identical(y[names(x)], x))
#    return(TRUE)
#  return(FALSE)
#},x$coVariables[131], bubble$coVariables[120]))

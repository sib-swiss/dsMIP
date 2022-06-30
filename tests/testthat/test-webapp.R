
freePipes <- function(pipeDir, maxWorkers, timeout){
  resPipes <- grep('res', dir(pipeDir), value = TRUE) # the workers have each their '.res' pipe
  npipes <- length(resPipes)
  found <- TRUE
  if(npipes >= maxWorkers){
    found <- FALSE
    for(f in resPipes){
      fRes <- paste0(pipeDir, '/', f)
      pRes <- txtq(fRes)
      lastM <- pRes$log()
      lastM <- lastM[nrow(lastM), c('title', 'time')]
      print(lastM)
      if(is.na(lastM$time) ||
         length(lastM$time) == 0 ||
         (as.numeric(Sys.time() - as.POSIXct(lastM$time)) < timeout && lastM$title !='timeout')){
        next
      }
      # if we are here we need to do some cleaning, first identify the req pipe as well
      fReq <- sub('\\.res', '.req', fRes)
      pReq <- txtq(fReq)
      lastR <- pReq$log()
      lastR <- lastR[nrow(lastR), c('title', 'time')]
      print(lastR)
      if(lastM$title == 'timeout' ||
         (
           !is.na(lastR$title) && length(lastR$title) > 0 && lastR$title == 'STOP'
          ) ){ # the listener is no longer with us, get rid of the pipes
        pReq$destroy()
        pRes$destroy()
        found <- TRUE
        next
      }
      if(as.numeric(Sys.time() - as.POSIXct(lastM$time)) >= timeout ){
        # first check if it's not working on something, that is if we have a more recent request
        if(is.na(lastR$time) ||
           length(lastR$time) == 0 ||
           as.POSIXct(lastR$time) < as.POSIXct(lastM$time)){ # probably nothing going on
          # send a stop message, if the listener is dead, next time the queues will be destroyed
          pReq$push('STOP', 'STOP')
          found <- TRUE
        }
      }
    }
  }
  return(found)
}

sentry <- function(user , password ){ # must return a head minion
  if(!freePipes(paste0(tempdir(TRUE), '/',config$dir), config$workers, 60)){
    stop("Too many connections")
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

sentryBackend <- SentryBackend$new(folder = config$dir, FUN = sentry)

sentryMw <- AuthMiddleware$new(
  auth_backend = sentryBackend,
  routes = "/",
  match = "partial",
  id = "sentry_middleware"
)

app <-  Application$new(content_type = "application/json",middleware = list(sentryMw))

app$add_get(
  path = "/login",
  FUN = function(req,res){
    bob <- req$cookies$minion
    req$cookies$minion <- NULL
    remoteLoad <- function(resourceMap, dfs){
      sapply(resourceMap$server, function(res){
        datashield.assign.resource(opals[res], sub('.','_',res, fixed = TRUE), res, async = FALSE)
      }) # resources are in
      # now dfs:
      sapply(dfs, function(x){
        where_clause <- NULL
        if(x == 'measurement'){
          where_clause <- 'value_as_number is not null'
        }
        dsqLoad(symbol= x, domain = 'concept_name', query_name = x, where_clause = where_clause, union = TRUE, datasources = opals)
      }) # done with the data frames
    } # remoteLoad
    nodeRes <- bob$getNodeResources()
    bob$sendRequest(remoteLoad, args = list(resourceMap = nodeRes, dfs = config$mainGroups), waitForIt = FALSE)

    prepareData <- function(){
      dssPivot(symbol = 'wide_m', what ='measurement', value.var = 'value_as_number',
               formula = 'person_id ~ measurement_name',
               by.col = 'person_id',
               fun.aggregate = function(x)x[1],
               datasources = opals)
      dssJoin(what = c('wide_m', 'person'),
              symbol = 'working_set',
              by = 'person_id',
              datasources = opals)
      n <- dssColNames('working_set')
      sapply(names(n), function(x){
        cnames <- n[[x]]
        cnames <- sub('measurement_name.', '', cnames, fixed = TRUE)
        dssColNames('working_set', cnames, datasources = opals[[x]])
      })
      ds.summary('working_set')

    }
    # launch the widening and join (async)
    bob$sendRequest(prepareData, waitForIt = FALSE)
    res$set_body('OK')
  })

app$add_get(
  path = "/getvars",
  FUN = function(req,res){
    cacheDir <- paste0(tempdir(check = TRUE), '/cache')
    if(!dir.exists(cacheDir)){
      dir.create(cacheDir)
    }
    nocache <- NULL
    if('nocache' %in% names(req$parameters_query)){
      nocache <- req$parameters_query[['nocache']] %>% tolower
    }

    if(!is.null(nocache) && nocache %in% c('true', 'yes')){
      cache <- FALSE
    } else {
      cache <- TRUE
    }
    # retrieve the minion:
    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]

    bob <- Minion$new(myUser, config$loginData, config$resourceMap)

    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    bob$reqQ <- txtq(paste0(qPath, '.req'))
    bob$resQ <- txtq(paste0(qPath, '.res'))


 if(!cache || !file.exists(paste0(cacheDir,'/bubble.json'))){
      # we have to do the work here.
    bubbleData <- list()
#    bubbleData$datasets <- lapply(config$loginData$server,function(x){
#      list(id = x, label  = x)
#    })
    bubbleData$datasets <- lapply(names(dssSwapKeys(config$resourceMap)),function(x){
            list(id = x, label  = x)
          })


    bubbleData$rootGroup <- list(id = 'root', label = 'Root Group', groups = c('person', 'measurement'))


    getVars <- function(grps){

      vars <- list(list(id ='date_of_birth', type = 'character'), list(id='gender', type = 'nominal'),
                   list(id = 'race', type = 'nominal'), list(id ='ethnicity', type = 'nominal'))
      grps <- setdiff(grps, 'person')
      p <- lapply(vars, function(var) var$id)
      vars_done <- c()


      grps <- sapply(grps, function(x){
        sapply(ds.levels(paste0(x, '$', x, '_name'), datasources = opals), function(y){
          make.names(y$Levels)
        }, simplify = FALSE)
      }, simplify = FALSE)

      varmap <- sapply(grps, dssSwapKeys, simplify = FALSE)
      varmap <- sapply(varmap, function(x) {

        sapply(x, function(y){
          list(type = 'number', cohorts = y)
        }, simplify = FALSE)

      }, simplify = FALSE)

      varmap <- Reduce(c, varmap) # keep only colname->cohorts (not dfname->colname->cohorts)
      person_cols <- ds.colnames('person', datasources = opals) %>% dssSwapKeys()
      person_cols <- person_cols[unlist(p)]

      varmap <- c(varmap, sapply(person_cols, function(x){   # add the person variables (all nominal)
        list(cohorts = x, type = 'nominal')
      }, simplify = FALSE))


      ################
      for (grp in grps) {
        for (db in grp) {
          for (var in db) {
            # Check if it's already on the list before adding it
            if (!(var %in% vars_done)) {
              vars_done <- append(vars_done, var) # Append var to the done list
              vars <- append(vars, list(list(id = var, type = "number")))
            }
          }
        }
      }

      #        grps <-  sapply(grps, function(x){
      #          sapply(x, function(db){
      #              unname(sapply(db, function(var){
      #                list(id = var, type = 'numeric')
      #              }, simplify = FALSE))

      #          }, simplify = FALSE)
      #        }, simplify = FALSE)
      grps$demographics <- p

      list(groups = grps, varmap = varmap, vars = vars)
    } # getvars

# debug:
   assign('kevin', bob, envir = .GlobalEnv)
    result <- bob$sendRequest(getVars, list(grps = config$mainGroups), timeout = 120)
    if(result$title == 'error'){
      stop(result$message)
    }

    bubbleData$variables <- result$message$var
    bubbleData$groups <- lapply(names(result$message$groups), function(x){
      list(id = x, label = x, variables =  result$message$groups[[x]])
    })
  #  bubbleData$groups[[length(bubbleData$groups)+1]] <- list(id = 'cohorts', label = 'Cohorts', variables = names(dssSwapKeys(config$resourceMap)) %>%  sub('\\..*','',.)) # without the 'db suffix', function(x){
    jsonBubble <- jsonlite::toJSON(bubbleData)
    jsonVarMap <- jsonlite::toJSON(result$message$varmap)
    save(jsonBubble, file = paste0(cacheDir, '/bubble.json'))
    save(jsonVarMap, file = paste0(cacheDir, '/varmap.json'))
 } else {
   load(file = paste0(cacheDir, '/bubble.json'))
   load(file = paste0(cacheDir, '/varmap.json'))
 }
    setvarmap <- function(x){
      assign('varmap', x, envir = .GlobalEnv)
    }
    varmap <- jsonlite::fromJSON(jsonVarMap)
    bob$sendRequest(setvarmap, list(varmap), waitForIt = FALSE)
    x <- jsonlite::fromJSON(jsonBubble, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
    jsonBubble <- jsonlite::toJSON(x, auto_unbox = TRUE)
    res$set_body(jsonBubble)
  }
)


app$add_get(
  path = "/quantiles",
  FUN = function(req,res){

    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]

    kevin <- Minion$new(myUser, config$loginData, config$resourceMap)

    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    kevin$reqQ <- txtq(paste0(qPath, '.req'))
    kevin$resQ <- txtq(paste0(qPath, '.res'))

    getQuants <- function(var, type , cohorts = NULL){
      op <- opals
      # only use the cohorts where we know we have this variable
      # varmap is a global list in forked process

      if(!is.null(varmap[[var]]$cohorts)){
        if(is.null(cohorts)){
          cohorts <- varmap[[var]]$cohorts
        } else {
          cohorts <- strsplit(cohorts, ',\\s*')[[1]]
          cohorts <- intersect(cohorts, varmap[[var]]$cohorts)
        }
      }

      op <-opals[cohorts]

      if(varmap[[var]]$type == 'number'){
        var = paste0('working_set$', var)

        ret <-  ds.quantileMean(var,type = type ,datasources = op)
        if(type == 'split'){
          if(length(names(op)) == 1){
            ret <- list(ret)
          }
          names(ret) <- names(op)
        } else {
          ret <- list(global = ret)
        }

      } else if(varmap[[var]]$type == 'nominal'){
        var = paste0('working_set$', var)
        ret <- ds.table1D(var,type = type , warningMessage = FALSE, datasources = op)$counts %>% unlist %>% list %>% sapply(function(b) as.list(b[,1]), simplify = FALSE)
        # unlist, list to handle global/vs split
        if(is.null(names(ret))){
          names(ret) <- 'global'
        }

      } else {
        stop(paste0('Not implemented for type ',varmap[[var]]$type ))
      }
      ret
    }

    params <- req$parameters_query
    if(!('var' %in% names(params))){
      stop('var is mandatory')
    }
    if(is.null(params$type)){
      params$type <- 'combine'
    }

    r<- kevin$sendRequest(getQuants, list(params$var, params$type, params$cohorts))

    res$set_body(jsonlite::toJSON(r$message, auto_unbox = TRUE))

})

app$add_get(
  path = "/histogram",
  FUN = function(req,res){

    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]
    kevin <- Minion$new(myUser, config$loginData, config$resourceMap)

    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    kevin$reqQ <- txtq(paste0(qPath, '.req'))
    kevin$resQ <- txtq(paste0(qPath, '.res'))

   f<- function(var, type , cohorts = NULL){
      op <- opals
      # only use the cohorts where we know we have this variable
      # varmap is a global list in forked process
      if(!is.null(varmap[[var]]$cohorts)){
        if(is.null(cohorts)){
          cohorts <- varmap[[var]]$cohorts
        } else {
          cohorts <- strsplit(cohorts, ',\\s*')[[1]]
          cohorts <- intersect(cohorts, varmap[[var]]$cohorts)
        }
      }

      op <-opals[cohorts]

     if(varmap[[var]]$type == 'number'){
       var = paste0('working_set$', var)
      ret <- ds.histogram(var,type = type ,datasources = op)
      if(type == 'split'){
        if(length(names(op)) == 1){
          ret <- list(ret)
        }
        names(ret) <- names(op)
      } else {
        ret <- list(global = ret)
      }

     } else if(varmap[[var]]$type == 'nominal'){
       var = paste0('working_set$', var)
       ret <- ds.table1D(var,type = type , warningMessage = FALSE, datasources = op)$counts %>% unlist %>% list %>% sapply(function(b) as.list(b[,1]), simplify = FALSE)
       # unlist, list to handle global/vs split
       if(is.null(names(ret))){
         names(ret) <- 'global'
       }

     } else {
       stop(paste0('Not implemented for type ',varmap[[var]]$type ))
     }
     ret
    }

    params <- req$parameters_query
    if(!('var' %in% names(params))){
      stop('var is mandatory')
    }
    if(is.null(params$type)){
      params$type <- 'combine'
    }

    r <- kevin$sendRequest(f, list(params$var, params$type, params$cohorts))
  #  if(params$type == 'split'){
    if(r$title == 'result'){
      out <- sapply(r$message, unclass, simplify = FALSE)
    } else{
  #    class(r$message) <-  'list'
   #   out <- r$message
      out <- r$message
   }
    res$set_body(jsonlite::toJSON(out, auto_unbox = TRUE))

  })


app$add_get(
  path = "/logout",
  FUN = function(req,res){

    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]

    kevin <- Minion$new(myUser, config$loginData, config$resourceMap)
    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    kevin$reqQ <- txtq(paste0(qPath, '.req'))
    kevin$resQ <- txtq(paste0(qPath, '.res'))
    ret <- kevin$stopProc()
    kevin$stopQueues()
    res$set_body(jsonlite::toJSON(ret, auto_unbox = TRUE))
  })
system.time(test_that("Login works", {
  ### make the request:

   credentials <- jsonlite::base64_enc("guest:guest123")
  headers <- list("Authorization" = sprintf("Basic %s", credentials))
  req <<- Request$new(
    path = "/login",
    parameters_query = list(nocache = 'true'),
    headers = headers
  )
  response <<- app$process_request(req)
  ck <<- list(user =  response$cookies$user$value, sid = response$cookies$sid$value)
  x <<- response$body
  expect_equal(x, 'OK')
}))

test_that(" Endpoint /getvars works", {


  ### make the request:
  req2 <- Request$new(
    path = "/getvars",
    parameters_query = list(nocache = 'true'),
    cookies = ck
  )
  response2 <- app$process_request(req2)
  x <<- jsonlite::fromJSON(response2$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(x$datasets[[1]]$id, 'omop_test.db')
})

test_that(" Endpoint /histogram works", {
  ### make the request:
  req2 <- Request$new(
    path = "/histogram",
    parameters_query = list(var = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", type = 'split', cohorts ="sophia.db"),
    cookies = ck
  )
  response3 <- app$process_request(req2)

  xxx<<- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(xxx), c(  "sophia.db"  ))
})

test_that(" Endpoint /histogram works for factors", {
  ### make the request:
  req2 <- Request$new(
    path = "/histogram",
    parameters_query = list(var = "ethnicity", type = 'split', cohorts ="sophia.db"),
    cookies = ck
  )
  response3 <- app$process_request(req2)

  xxx<<- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(xxx), c(  "sophia.db"  ))
})



test_that(" Endpoint /quantiles works", {
  ### make the request:
  req2 <- Request$new(
    path = "/quantiles",
    parameters_query = list(var = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", type = 'combine'),
    cookies = ck
  )
  response3 <- app$process_request(req2)
  x<<- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(x), c('global' ))
})

test_that(" Endpoint /quantiles works for factors", {
  ### make the request:
  req2 <- Request$new(
    path = "/quantiles",
    parameters_query = list(var = "ethnicity", type = 'combine', cohorts = 'sophia.db'),
    cookies = ck
  )
  response3 <- app$process_request(req2)
  x<<- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(x), c('global' ))
})



#test_that(" Endpoint /logout works", {
  ### make the request:
#  req2 <- Request$new(
#    path = "/logout",
#    cookies = ck
#  )
#  response4 <- app$process_request(req2)
#  xx<<- jsonlite::fromJSON(response4$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
#  expect_equal(xx[['title']], c('STOP' ))
#})


ff <- function() varmap
ff <- function() ds.colnames('working_set')
ff <- function() ds.table1D('working_set$ethnicity', type ='split', warningMessage = FALSE, datasources = opals['sophia.db', 'omop_test.db'])
ff<- function(var, type , cohorts = NULL){
  op <- opals
  # only use the cohorts where we know we have this variable
  # varmap is a global list in forked process

  if(!is.null(varmap[[var]]$cohorts)){
    if(is.null(cohorts)){
      cohorts <- varmap[[var]]$cohorts
    } else {
      cohorts <- strsplit(cohorts, ',\\s*')[[1]]
      cohorts <- intersect(cohorts, varmap[[var]]$cohorts)
    }
  }

  op <-opals[cohorts]

  if(varmap[[var]]$type == 'number'){
    var = paste0('working_set$', var)

    ret <-  ds.quantileMean(var,type = type ,datasources = op)
    if(type == 'split'){
      if(length(names(op)) == 1){
        ret <- list(ret)
      }
      names(ret) <- names(op)
    } else {
      ret <- list(global = ret)
    }

  } else if(varmap[[var]]$type == 'nominal'){
    var = paste0('working_set$', var)

    ret <- ds.table1D(var,type = type , warningMessage = FALSE, datasources = op)$counts %>% unlist %>% list %>% sapply(function(b) as.list(b[,1]), simplify = FALSE)

  } else {
    stop(paste0('Not implemented for type ',varmap[[var]]$type ))
  }
ret
}
ff <- function() ds.colnames('person') %>% dssSwapKeys()

x <-kevin$sendRequest(ff)
x <- kevin$sendRequest(ff, list("ethnicity", type = 'combine', cohorts ="sophia.db, test.db"))$message

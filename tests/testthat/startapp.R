devtools::load_all()
source('./setup.R')


  makeJson <- function(thisSession, cache = TRUE){
    if(!cache || !file.exists('/tmp/bubbleVars')){
      vars <- thisSession$getVars()
      reshaped <- thisSession$reshapeVars(vars,list(person = c('date_of_birth','gender', 'race','ethnicity')))
      save(vars, reshaped, file = '/tmp/bubbleVars')
      assign('vars', vars, envir = .GlobalEnv)
      assign('reshaped', reshaped, envir = .GlobalEnv)
    } else {
      if(!all(c('vars', 'reshaped') %in% names(.GlobalEnv))){
        load('/tmp/bubbleVars')
        assign('vars', vars, envir = .GlobalEnv)
        assign('reshaped', reshaped, envir = .GlobalEnv)
      }
    }
    bubbleData <- list()
    bubbleData$datasets <- lapply(config$loginData$server,function(x){
      list(id = x, label  = x)
    })

    bubbleData$groups <- lapply(names(reshaped), function(x){
      list(id = x, label = x, variables =  reshaped[[x]], groups = 'cohorts')
    })
    bubbleData$groups[[length(bubbleData$groups)+1]] <-  list(id = 'cohorts', label = 'Cohorts', names(dssSwapKeys(config$resourceMap)) %>%  sub('\\..*','',.)) # without the 'db suffix', function(x){
    bubbleData$rootGroup <- list(id = 'root', label = 'Root Group', groups = c('person', 'measurement', 'observation'))
    toJSON(bubbleData, auto_unbox = TRUE)
  }


 prepareData <- function(sess, async = TRUE){
    objs <- sess$minionEval(expression(datashield.symbols(opals)))
    ##### person
    persName <- sapply(objs, function(x) grep('person',x, value = TRUE))
    upsideDownPers <- dssSwapKeys(persName) # for each common name execute simultaneously on multiple servers
    sapply(names(upsideDownPers), function(pName){
      ds <- paste(upsideDownPers[[pName]], collapse = "', '")
      ds <- paste0("'", ds, "'")
      expr <- paste0('dssSubset(',
                     "symbol = '", pName,"',",
                      "what = '", pName, "',",
                      "col.filter = 'c(\"person_id\", \"gender\", \"birth_datetime\", \"race\",\"ethnicity\", \"database\")',",
                      "datasources = opals[c(", ds, ")])")
      sess$minionEval(parse(text = expr), async = async)
    })

    ###### measurement (here we do the join too)
    measurementName <- sapply(objs, function(x) grep('measurement',x, value = TRUE))
    upsideDownMeasurement <- dssSwapKeys(measurementName) # for each common name execute simultaneously on multiple servers
    sapply(names(upsideDownMeasurement), function(mName){
      ds <- paste(upsideDownMeasurement[[mName]], collapse = "', '")
      ds <- paste0("'", ds, "'")
      wideSym <- paste0('w_', mName)
      pivotExpr <- paste0('dssPivot(',
                          "symbol = '", wideSym,"', what ='", mName,"', value.var = 'value_as_number',
                     formula = 'person_id ~ measurement_name',
                     by.col = 'person_id',
                     fun.aggregate = function(x)x[1],
                     datasources = opals[c(", ds, ")])")
      sess$minionEval(parse(text = pivotExpr), async = async)
      # now join with person
      pName <- sub('measurement', 'person', mName, fixed = TRUE)
      joinExpr <- paste0("dssJoin(",
                         "what = c('", wideSym, "', '", pName, "'),
                    symbol = 'working_set',
                    by = 'person_id',
                    datasources = opals[c(", ds, ")])")
      sess$minionEval(parse(text = joinExpr), async = async)

    })

  }


  sentry <- function(user , password ){ # must return a session, the user/pass check is delegated to the nodes
	  warning('authenticating...')
    WebSession$new(minionShop = minionHerd, usr = user, pass = password, logindata = config$loginData,resources = config$resourceMap, groups = config$mainGroups)
  }

  sentryBackend <- SentryBackend$new(FUN = sentry)

  sentryMw <- AuthMiddleware$new(
    auth_backend = sentryBackend,
    routes = "/",
    match = "partial",
    id = "sentry_middleware"
  )

  app <-  Application$new(content_type = "application/json",middleware = list(sentryMw))
  app$add_get(
    path = "/start",
    FUN = function(req,res){
      nocache <- NULL
      if('nocache' %in% names(req$parameters_query)){
        nocache <- req$parameters_query[['nocache']] %>% tolower
      }

      if(!is.null(nocache) && nocache %in% c('true', 'yes')){
        cache <- FALSE
      } else {
        cache <- TRUE
      }
      assign('a', a+1, envir = .GlobalEnv)
      b <- get('a', envir =  .GlobalEnv)
      warning(b)
      res$set_body(as.character(b))
    #  thisSess <- sessionList[[req$cookies$siid$value]]
    #  out <- makeJson(thisSess, cache)
      # launch the widening and join (async) before returning
    #  prepareData(thisSess)
    #  res$set_body(out)

    }
  )
backend <- BackendRserve$new()
backend$start(app, http_port = config$port)

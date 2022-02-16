test_that("Web app start works", {
  # first, unleash the minions:
  minionHerd <- MinionHive$new(config$workers, config$minWorkers, config$maxWorkers, config$addWorkers, c('dsBaseClient', 'dsSwissKnifeClient', 'dsQueryLibrary', 'dsResource'))
  sessionList <<- list()
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
    WebSession$new(minionShop = minionHerd, usr = user, pass = password, logindata = config$loginData,resources = config$resourceMap, groups = config$mainGroups)
  }

  sentryBackend <- SentryBackend$new(FUN = sentry)

  sentryMw <- AuthMiddleware$new(
    auth_backend = sentryBackend,
    routes = "/",
    match = "partial",
    id = "sentry_middleware"
  )

  app <<-  Application$new(content_type = "application/json",middleware = list(sentryMw))
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
      thisSess <- sessionList[[req$cookies$sid$value]]
      out <- makeJson(thisSess, cache)
      # launch the widening and join (async) before returning
      prepareData(thisSess)
      res$set_body(out)

    }
  )

  expect_equal(app$endpoints$GET, c(exact = '/start'))



  app$add_get(
    path = "/means",
    FUN = function(req,res){
      thisSession <- sessionList[[req$cookies$sid$value]]
      out <- thisSession$minionCall(dssColMeans, list('working_set', async = FALSE))
      assign('mns', out, envir = .GlobalEnv)
      out <- jsonlite::toJSON(out)
      res$set_body(out)
   }
)
expect_equal(app$endpoints$GET, c(exact = '/start', exact = '/means'))

})

app$add_get(
  path = "/quantiles",
  FUN = function(req,res){

    qVars <- jsonlite::fromJSON(req$parameters_query[['vars']])
    nodes <- NULL
    cohorts <- NULL

    thisSession <- sessionList[[req$cookies$sid$value]]
    out <- thisSession$minionCall(dssColMeans, list('working_set', async = FALSE))
    assign('mns', out, envir = .GlobalEnv)
    out <- jsonlite::toJSON(out)
    res$set_body(out)
  }
)
expect_equal(app$endpoints$GET, c(exact = '/start', exact = '/means'))

})


test_that("Login works", {
### make the request:
  credentials = jsonlite::base64_enc("guest:guest123")
  headers = list("Authorization" = sprintf("Basic %s", credentials))
  req = Request$new(
    path = "/start",
    parameters_query = list(nocache = 'true'),
    headers = headers
  )
  response <<- app$process_request(req)
  x <<- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(x$groups[[1]]$label[[1]], 'person')
})

test_that("Second request works", {
  ### make the request:

  req2 <-  Request$new(
    path = "/start",
    parameters_query = list(nocache = 'true'),
    cookies = response$cookies
  )
  response2 <- app$process_request(req2)
  x <- jsonlite::fromJSON(response2$body,  simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(x$groups[[1]]$label[[1]], 'person')
  suppressWarnings(gc(FALSE))
})

test_that("/means endpoint works", {
  ### make the request:

  req3 <-  Request$new(
    path = "/means",
    parameters_query = list(nocache = 'true'),
    cookies = response$cookies
  )
  response3 <- app$process_request(req3)
  x <- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(x$global$nrows, 1225)
  suppressWarnings(gc(FALSE))
})

sess <- sessionList[[1]]
sess$minionEval(expression(datashield.symbols(opals)))
sess$minionEval(expression(datashield.errors()))
sess$minionEval(expression())
y <- sess$minionEval(expression(datashield.aggregate(opals, quote(selfUpgrade(NULL,NULL,NULL ,TRUE)), async = FALSE)))

cls <- sess$minionEval(expression(dssColNames('working_set')))
cls <- Reduce(union, cls)
str(cls)
str(x$groups)


xvars <- sapply(x$groups, function(y)  y$variables)[2] %>% Reduce(union,.)
cls <- sub('measurement_name\\.','', cls)
setdiff(cls, xvars)
setdiff(xvars, cls)

x <- sess$vars[[1]]

y <- x %>% dssSwapKeys %>% sapply(dssSwapKeys, simplify = FALSE) %>% dssSwapKeys
str(y)



xx <- httr::GET('http://localhost:8888/start',
          accept_json(),
          add_headers('Authorization' = sprintf("Basic %s", credentials)), verbose())
xx <- httr::GET('http://www.yahoo.com',
                accept_json(),
                add_headers('Authorization' = 'a'), verbose())
s <- x$cookies[x$cookies$name == 'sid', 'value']
x <- httr::GET('http://localhost:8888/start',
               accept_json(),
               set_cookies(.cookies = c(sid = 'f33c47')))




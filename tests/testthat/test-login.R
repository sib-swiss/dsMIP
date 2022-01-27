test_that("Web app start works", {
  sessionList <<- list()
  bubbleData <- list()
  bubbleData$datasets <- lapply(config$loginData$server,function(x){
    list(id = x, label  = x)
  })

 cohortGroup <- list(id = 'cohorts', label = 'Cohorts', names(dssSwapKeys(config$resourceMap)) %>%  sub('\\..*','',.)) # without the 'db suffix', function(x){
 bubbleData$rootGroup <- list(id = 'root', label = 'Root Group', groups = c('person', 'measurement', 'observation'))

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
      thisSession <- sessionList[[req$cookies$sid$value]]
      makeJson <- function(){
        vars <- thisSession$getVars()
        assign('myVars', vars, envir = .GlobalEnv)

        bubbleData$groups <<- lapply(names(vars), function(x){
          list(id = x, label = x, variables =  vars[[x]], groups = 'cohorts')
        })
        bubbleData$groups[[length(bubbleData$groups)+1]] <- cohortGroup
        assign('bubbleJson', toJSON(bubbleData, auto_unbox = TRUE), envir = .GlobalEnv)
        save(bubbleJson, file = '../cache/bubble.json')
      }
      nocache <- NULL
      if('nocache' %in% names(req$parameters_query)){
        nocache <- req$parameters_query[['nocache']] %>% tolower
      }

      if(!is.null(nocache) && nocache %in% c('true', 'yes')){
        makeJson()
        out <- bubbleJson
      } else {
        if(exists('bubbleJson', envir = .GlobalEnv)){
          out <- bubbleJson
        } else {
          if(file.exists('../cache/bubble.json')){
            load('../cache/bubble.json', envir = .GlobalEnv)
            out <- bubbleJson
          } else {
            makeJson()
            out <- bubbleJson
          }
        }
      }
      # launch the widening and join (async) before returning
      prepareData(thisSession)
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
  x <- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
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




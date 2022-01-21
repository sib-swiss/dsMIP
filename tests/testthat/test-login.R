test_that("Web app start works", {
  sessionList <<- list()
  bubbleData <- list()
  bubbleData$datasets <- lapply(config$loginData$server,function(x){
    list(id = x, label  = x)
  })


  getVars <- function(groups){
    ret <- list(person = c('date_of_birth','gender', 'race','ethnicity'))
    groups <- setdiff(groups, 'person')
    out <- sapply(groups, function(x){
      sapply(ds.levels(paste0(x,'$',x,'_name')), function(y) y$Levels, simplify = FALSE)
    }, simplify = FALSE)
    c(ret,out)
  }

  prepareData <- function(){
    objs <- sess$minionEval(expression(datashield.symbols(opals)))
    ##### person
    persName <- sapply(objs, function(x) grep('person',x, value = TRUE))
    upsideDownPers <- dssSwapKeys(persName) # for each common name execute simultaneously on multiple servers
    sapply(names(upsideDownPers), function(pName){
      ds <- paste(upsideDownPers[[pName]], collapse = "', '")
      ds <- paste0("'", ds, "'")
      expr <- paste0('dssPivot(',
                     "symbol = pName,
                     what = pName,
                     col.filter = 'c(\"person_id\", \"gender\", \"birth_datetime\", \"race\",\"ethnicity\", \"database\")',
                     datasources = opals[c(", ds, ")]")
      sess$minionEval(parse(text = expr), async = TRUE)
    })

    ###### measurement (here we do the join too)
    measurementName <- sapply(objs, function(x) grep('measurement',x, value = TRUE))
    upsideDownMeasurement <- dssSwapKeys(measurementName) # for each common name execute simultaneously on multiple servers
    sapply(names(upsideDownMeasurement), function(mName){
      ds <- paste(upsideDownMeasurement[[mName]], collapse = "', '")
      ds <- paste0("'", ds, "'")
      wideSym <- paste0('w_', mName)
      pivotExpr <- paste0('dssPivot(',
                          "symbol = ", wideSym,
                          ", what =", mname,
                          ", value.var = 'value_as_number',
                     formula = 'person_id ~ measurement_name',
                     by.col = 'person_id',
                     fun.aggregate = function(x)x[1],
                     datasources = opals[c(", ds, ")]")
      sess$minionEval(parse(text = pivotExpr), async = TRUE)
      # now join with person
      pName <- sub('measurement', 'person', mName, fixed = TRUE)
      joinExpr <- paste0("dssJoin(",
                         "what = c('", wideSym, "', 'person'),
                    symbol = 'working_set',
                    by = 'person_id',
                    datasources = opals[c(", ds, ")]")
      sess$minionEval(parse(text = joinExpr), async = TRUE)

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
          vars <- thisSession$minionCall(getVars, list(config$mainGroups))
          bubbleData$groups <<-lapply(names(vars), function(x){
            list(id = x, label = x, variables = Reduce(union, vars[[x]]))
          })
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
       # prepareData()
        res$set_body(out)

    }
  )
  expect_equal(app$endpoints$GET, c(exact = '/start'))



  app$add_get(
    path = "/means",
    FUN = function(req,res){
      thisSession <- sessionList[[req$cookies$sid$value]]
      out <- thisSession$minionCall(dssColMeans, list('working_set'))
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
  expect_equal(x$groups$label[[1]], 'person')
})

test_that("Second request works", {
  ### make the request:

  req2 <-  Request$new(
    path = "/start",
    parameters_query = list(nocache = 'true'),
    cookies = response$cookies
  )
  response2 <- app$process_request(req2)
  x <- jsonlite::fromJSON(response2$body)
  expect_equal(x$groups$label[[1]], 'person')
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
  x <- jsonlite::fromJSON(response3$body)
  expect_equal(x$groups$label[[1]], 'person')
  suppressWarnings(gc(FALSE))
})


for(i in setdiff(dir('/home/iulian/R/x86_64-pc-linux-gnu-library/3.6'), dir('/home/iulian/R/x86_64-pc-linux-gnu-library/4.1/'))){
  try(install.packages(i))
}

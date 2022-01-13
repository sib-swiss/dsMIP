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
          vars <- thisSession$minionCall(getVars, config$mainGroups)
          bubbleData$groups <<-lapply(names(vars), function(x){
            list(id = x, label = x, variables = Reduce(union, vars[[x]]))
          })
          assign('bubbleJson', toJSON(bubbleData), envir = .GlobalEnv)
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

        res$set_body(out)

    }
  )
  expect_equal(app$endpoints$GET, c(exact = '/start'))

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
  x <- jsonlite::fromJSON(response$body)
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
})


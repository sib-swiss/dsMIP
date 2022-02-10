sentry <- function(user , password ){ # must return a head minion
  carl <- HeadMinion$new(user,config$libraries)
  carl$startQueues(config$dir)
  carl$startProc()
  carl$loadLibs()
  logged <- carl$login(password, config$loginData, TRUE)

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
    # retrieve the minion:
    bob <- req$cookies$minion
    req$cookies$minion <- NULL
    remoteLoad <- function(resourceMap, dfs){
      sapply(names(resourceMap), function(srv){
        sapply(resourceMap[[srv]], function(res){
          datashield.assign.resource(opals[srv], sub('.','_',res, fixed = TRUE), res, async = FALSE)
        })
      }) # resources are in
      # now dfs:
      sapply(dfs, function(x){
        where_clause <- NULL
        if(x == 'measurement'){
          where_clause <- 'value_as_number is not null'
        }
        dsqLoad(symbol= x, domain = 'concept_name', query_name = x, where_clause = where_clause, union = TRUE, datasources = opals)
      }) # done with the data frames
      datashield.symbols(opals)
    }


#    if(!cache){
      # we have to do the work here.
    bob$sendRequest(remoteLoad, args = list(resourceMap = config$resourceMap, dfs = config$mainGroups), waitForIt = FALSE)
    getVars <- function(grps, rs){
      ret <- list(person = c('date_of_birth','gender', 'race','ethnicity'))
      grps <- setdiff(grps, 'person')


        sapply(grps, function(x){
          make.names(ds.levels(paste0(x, '$', x, '_name'), datasources = opals))

        }, simplify = FALSE)

      }




    result <- bob$sendRequest(getVars, list(grps = config$mainGroups, rs = config$resourceMap), timeout = 120)

    reshapeVars = function(vars, moreVars){
      realGrps <- sapply(vars, function(serverlist){
        sapply(serverlist, function(srv){
          Reduce(union, srv)
        }, simplify = FALSE) %>% Reduce(union, .)
      }, simplify = FALSE)
      c(moreVars, realGrps)
    }
    #  thisSess <- sessionList[[req$cookies$siid$value]]
    #  out <- makeJson(thisSess, cache)
    # launch the widening and join (async)  returning
    #  prepareData(thisSess)
     res$set_body(jsonlite::toJSON(result$message))

  }
)

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


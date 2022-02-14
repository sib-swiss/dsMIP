sentry <- function(user , password ){ # must return a head minion
  pipeDir <- paste0(tempdir(TRUE), '/',config$dir)
  npipes <- length(grep('err', dir(pipeDir))) # the workers have each their '_err' file
  if(npipes >= config$workers){
    stop('Too many connections')
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
      datashield.symbols(opals)
    }


#    if(!cache){
      # we have to do the work here.
    bubbleData <- list()
    bubbleData$datasets <- lapply(config$loginData$server,function(x){
      list(id = x, label  = x)
    })

    bubbleData$rootGroup <- list(id = 'root', label = 'Root Group', groups = c('person', 'measurement', 'observation'))

    x <- bob$sendRequest(remoteLoad, args = list(resourceMap = bob$getNodeResources(), dfs = config$mainGroups), waitForIt = TRUE)

    getVars <- function(grps, rs){
      p <- list(person = c('date_of_birth','gender', 'race','ethnicity'))
      grps <- setdiff(grps, 'person')


              grps <- sapply(grps, function(x){
          sapply(ds.levels(paste0(x, '$', x, '_name'), datasources = opals), function(y){
             y$Levels
          }, simplify = FALSE) %>% Reduce(union,.) %>% make.names
         }, simplify = FALSE)

        grps$person <- p
        grps
      } # getvars




    result <- bob$sendRequest(getVars, list(grps = config$mainGroups, rs = config$resourceMap), timeout = 120)
    if(result$title == 'error'){
      stop(result$message)
    }
    bubbleData$groups <- lapply(names(result$message), function(x){
      list(id = x, label = x, variables =  result$message[[x]], groups = 'cohorts')
    })
    bubbleData$groups[[length(bubbleData$groups)+1]] <- list(id = 'cohorts', label = 'Cohorts', names(dssSwapKeys(config$resourceMap)) %>%  sub('\\..*','',.)) # without the 'db suffix', function(x){
    reshapeVars = function(vars, moreVars){
      realGrps <- sapply(vars, function(serverlist){
        sapply(serverlist, function(srv){
          Reduce(union, srv)
        }, simplify = FALSE) %>% Reduce(union, .)
      }, simplify = FALSE)
      c(moreVars, realGrps)
    }


    # prepareData

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
      ds.summary('working_set')

    }

    # launch the widening and join (async)  returning
    bob$sendRequest(prepareData, waitForIt = FALSE)
    res$set_body(jsonlite::toJSON(bubbleData))

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
  response <- app$process_request(req)
  x <<- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(x$rootGroup$groups, c("person", "measurement", "observation"))
})




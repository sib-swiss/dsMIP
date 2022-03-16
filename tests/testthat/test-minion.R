test_that("we can wake up a minion, run a remote call and get the result", {

  carl <- Minion$new()

  msg <- jsonlite::serializeJSON(list(func = sessionInfo))
  carl$reqQ$push('info', msg)
  out <- carl$blockingRead()
  expect_true('txtq' %in% names(out$otherPkgs))
})

test_that("the minion can login/logout", {

  carl <- HeadMinion$new('guest', config$loginData, config$resourceMap,  config$libraries)
  carl$startQueues(config$dir)
  carl$startProc()
  carl$loadLibs()
  logged <- carl$login('guest123', TRUE)
  devSetOptions <- function(){ # only in dev
    dssSetOption(list('cdm_schema' = 'synthea_omop'))
    dssSetOption(list('vocabulary_schema' = 'omop_vocabulary'))
  }
  carl$sendRequest(devSetOptions, waitForIt = TRUE)
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

  nodeRes <- carl$getNodeResources()
  x <- carl$sendRequest(remoteLoad, args = list(resourceMap = nodeRes, dfs = config$mainGroups), waitForIt = TRUE)

})

getVars <- function(grps, rs){
  p <- c('date_of_birth','gender', 'race','ethnicity')
  grps <- setdiff(grps, 'person')


  grps <- sapply(grps, function(x){
    sapply(ds.levels(paste0(x, '$', x, '_name'), datasources = opals), function(y){
      y$Levels
    }, simplify = FALSE) %>% Reduce(union,.) %>% make.names
  }, simplify = FALSE)

  grps$person <- p
  grps
} # getvars




result <- carl$sendRequest(getVars, list(grps = config$mainGroups, rs = config$resourceMap), timeout=120)
carl$sendRequest(function() ds.histogram('working_set$race'))

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

carl$sendRequest(renameCols)

renameCols <- function(){
  n <- dssColNames('working_set')
  sapply(names(n), function(x){
    cnames <- n[[x]]
    cnames <- sub('measurement_name.', '', cnames, fixed = TRUE)
    dssColNames('working_set', cnames, datasources = opals[[x]])
  })
}

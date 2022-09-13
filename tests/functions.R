resMap <- data.frame(
  server = c( "accelerate.db", "rewind.db", "extend.db" ),
  url = c(rep('https://sophia-fdb.vital-it.ch/sib',2), 'https://sophia-fdb.vital-it.ch/extend'),
  node = c(rep('sib',2), 'extend')
)

resMap$user <-getOption('datashield.username')
resMap$password <-getOption('datashield.password')

opals <- datashield.login(resMap)

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

remoteLoad(resMap, c('measurement', 'person'))

getVars <- function(grps){

  vars <- list(list(id ='date_of_birth', type = 'character'), list(id='gender', type = 'nominal'),
               list(id = 'race', type = 'nominal'), list(id ='ethnicity', type = 'nominal'))
  grps <- setdiff(grps, 'person')
  p <- lapply(vars, function(var) var$id)
  vars_done <- c()


  grps <- sapply(grps, function(x){
    sapply(ds.levels(paste0(x, '$', x, '_name'), datasources = opals), function(y){
      make.names(y$Levels)
    }, simplify = FALSE)# %>% Reduce(union,.) %>% make.names
  }, simplify = FALSE)

  varmap <- sapply(grps, dssSwapKeys, simplify = FALSE)
  varmap <- sapply(varmap, function(x) {
 #   names(x) <- make.names(names(x))
    sapply(x, function(y){
      list(type = 'number', cohorts = y)
    }, simplify = FALSE)
  #  x
  }, simplify = FALSE)

  varmap <- Reduce(c, varmap) # keep only colname->cohorts (not dfname->colname->cohorts)
  for(demcol in unlist(p)){
    varmap[[demcol]] <- list(type = 'nominal')
  }

  #   grps <- sapply(grps, function(x)Reduce(union, x) %>% make.names, simplify = FALSE)
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
confFile <- '../config.json'
config <- readChar(confFile, file.info(confFile)$size) %>%
  gsub('(?<=:)\\s+|(?<=\\{)\\s+|(?<=,)\\s+|(?<=\\[)\\s+','',., perl = TRUE) %>%
  fromJSON()

a <- getVars(config$mainGroups)

y <- sapply(varmap,function(x){
  list(type = 'number', cohorts = x)
}, simplify = FALSE)

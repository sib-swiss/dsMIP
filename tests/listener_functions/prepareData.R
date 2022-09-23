 prepareData <- function(){
   # opals in already in the global env
  #first the resources:
  sapply(names(opals), function(res){
    datashield.assign.resource(opals[res], sub('.','_',res, fixed = TRUE), res, async = FALSE)
  })
  # load the 2 data frames
  ################ !!!!!!!!!!!!!!!!!!!!!! ############### only for development!!!!!
  dssSetOption(list('cdm_schema' = 'synthea_omop'), datasources = opals)
  dssSetOption(list('vocabulary_schema' = 'omop_vocabulary'), datasources = opals)
  ##########################################
  tryCatch(dsqLoad(symbol= 'measurement',
                   domain = 'concept_name',
                   query_name = 'measurement',
                   where_clause = 'value_as_number is not null',
                   #  row_limit =  3000000, ## tayside doesn't handle more
                   row_limit =  300, ## dev only
                   union = TRUE,
                   datasources = opals), error = function(e){
                     stop(datashield.errors())
                   })
  dsqLoad(symbol= 'person',
          domain = 'concept_name',
          query_name = 'person',
          union = TRUE,
          datasources = opals)
  ################ !!!!!!!!!!!!!!!!!!!!!! ############### only for development!!!!!
  dssDeriveColumn('measurement', 'measurement_date', '"12-11-2005"', datasources = opals)
  ##########################################


  # fix funky measurement dates:

  dssSubset('measurement', 'measurement', row.filter = 'measurement_date >= "01-01-1970"', datasources = opals)

  ############## calculate age #####################
  # order by measurement date for each person_id
  dssSubset('measurement', 'measurement', 'order(person_id, measurement_date)', async = TRUE, datasources = opals)
  #  measurement dates as numbers:
  dssDeriveColumn('measurement', 'measurement_date_n', 'as.numeric(as.Date(measurement_date))', datasources = opals)
  # add a dummy column just for the widening formula, this will hold eventually the 'aggregate' first measurement date
  dssDeriveColumn('measurement', 'f', '"irst_measurement_dat.e"', datasources = opals)
  # now we can widen by that column and pick the first value:
  dssPivot(symbol = 'first_m_dates', what ='measurement', value.var = 'measurement_date_n',
           formula = 'person_id ~ f',
           by.col = 'person_id',
           fun.aggregate = function(x)x[1], # we are sure it's the first date, baseline, they've been ordered
           async = TRUE,
           datasources = opals)

  dssJoin(c('person', 'first_m_dates'), symbol= 'person', by = 'person_id', join.type = 'inner', datasources = opals)
  try(datashield.rm(datasources, 'first_m_dates'), silent = TRUE) # keep it slim
  # now calculate the age at first measurement:
  dssDeriveColumn('person', 'age', 'round((f.irst_measurement_dat.e - as.numeric(as.Date(birth_datetime)))/365)', datasources = opals)

  ###################  finished with age ##########################################

  dssPivot(symbol = 'wide_m', what ='measurement', value.var = 'value_as_number',
           formula = 'person_id ~ measurement_name',
           by.col = 'person_id',
           fun.aggregate = function(x)x[1], # maybe we'll want mean here?
           datasources = opals)
  try(datashield.rm(datasources, 'measurement'), silent = TRUE)
  dssJoin(what = c('wide_m', 'person'),
          symbol = 'working_set',
          by = 'person_id',
          datasources = opals)
  dssSubset('working_set', 'working_set',
            col.filter = 'setdiff(colnames(working_set), c("database", "f.irst_measurement_dat.e", "birth_datetime" , "location_id", "provider_id" , "care_site_id", "person_id")) ',
            datasources = opals) # get rid of superfluous columns
  try(datashield.rm(opals, 'person'), silent = TRUE)
  try(datashield.rm(opals, 'wide_m'), silent = TRUE)
  #### fix column names:
  n <- dssColNames('working_set', datasources = opals)
  sapply(names(n), function(x){
    cnames <- n[[x]]
    cnames <- sub('measurement_name.', '', cnames, fixed = TRUE)
    dssColNames('working_set', cnames, datasources = opals[[x]])
  })
  # create varmap:
  varsToCohorts <- dssSwapKeys(n)
  varsToCohorts <- sapply(varsToCohorts, function(a){
    list(type = 'number', cohorts = a)
  }, simplify = FALSE)
  varsToCohorts[c('ethnicity', 'race', 'gender')] <- sapply(varsToCohorts[c('ethnicity', 'race', 'gender')], function(a){
    list(type = 'nominal', cohorts = a$cohorts)
  }, simplify = FALSE)
  assign('varmap', varsToCohorts, envir = .GlobalEnv)
  return(varsToCohorts)
}

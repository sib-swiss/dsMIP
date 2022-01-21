test_that("MinionHive works", {
  minionHerd <<- MinionHive$new(config$workers, config$minWorkers, config$maxWorkers, config$addWorkers, c('dsBaseClient', 'dsSwissKnifeClient', 'dsQueryLibrary', 'dsResource'))
  expect_equal(minionHerd$team, config$addWorkers) # first should have been set from second
})

test_that("WebSession works", {
  sess <- WebSession$new(minionShop = minionHerd, usr = 'guest', pass = 'guest123', logindata = config$loginData,resources = config$resourceMap, groups = config$mainGroups)
  expect_equal(sess$minionEval(expression(datashield.symbols(opals)))$server1[1], 'measurement')
})

y <-list()
while(length(x <- sess$lastAsyncResults(2)) >0 ){
  y <- c(y,x)
}
y
x <- sess$getMinion()
ids = list(sess$minionEval(expression(datashield.symbols(opals)), async = TRUE),sess$minionEval(expression(datashield.symbols(opals1)), async = TRUE))
sess$getResult(ids[[1]])

y <- sess$minionEval(expression(datashield.aggregate(opals, quote(selfUpgrade('dsQueryLibraryServer',NULL,NULL ,TRUE)), async = FALSE)))
y <- sess$minionEval(expression(datashield.aggregate(opals, quote(selfUpgrade('resourcex',NULL,NULL ,TRUE)), async = FALSE)))

sess$minionEval(expression(datashield.symbols(opals)))
sess$minionEval(expression(datashield.errors()))
sess$minionEval(expression(ds.summary('measurement', datasources = opals[1])))
sess$minionEval(expression(dssShowFactors('measurement', datasources = opals[1])))
sess$minionCall(getVars, list(config$mainGroups))
pivotArgs <- list( symbol = 'w_measurement',
                   what = 'measurement',
                   value.var = 'value_as_number',
                   formula = 'person_id ~ measurement_name',
                   by.col = 'person_id',
                   fun.aggregate = function(x)x[1])
sess$minionCall(dssPivot, pivotArgs)
sess$minionCall(datashield.errors, list())
sess$minionEval(expression(datashield.errors()))
sess$minionCall(ds.summary, list('working_set'))
sess$minionEval(expression(ds.summary('w_measurement', datasources = opals)))
x <- sess$minionCall(ds.class, list('working_set$measurement_name.Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma_database.sophia_db'))

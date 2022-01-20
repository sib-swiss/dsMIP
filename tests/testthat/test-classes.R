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
sess$minionEval(expression(ds.summary('measurement$database', datasources = opals['server'])))
sess$minionEval(expression(dssShowFactors('measurement', datasources = opals[2])))
sess$minionCall(getVars, list(config$mainGroups))

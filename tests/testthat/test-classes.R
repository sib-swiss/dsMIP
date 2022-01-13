test_that("MinionHive works", {
  minionHerd <<- MinionHive$new(config$workers, config$minWorkers, config$maxWorkers, config$addWorkers, c('dsBaseClient', 'dsSwissKnifeClient', 'dsQueryLibrary', 'dsResource'))
  expect_equal(minionHerd$team, config$addWorkers) # first should have been set from second
})

test_that("WebSession works", {
  sess <- WebSession$new(minionShop = minionHerd, usr = 'guest', pass = 'guest123', logindata = config$loginData,resources = config$resourceMap, groups = config$mainGroups)
  expect_equal(sess$minionEval(expression(datashield.symbols(opals)))$server1[1], 'measurement')
})


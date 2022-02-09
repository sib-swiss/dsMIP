test_that("we can wake up a minion, run a remote call and get the result", {

  carl <- Minion$new()

  msg <- jsonlite::serializeJSON(list(func = sessionInfo))
  carl$reqQ$push('info', msg)
  out <- carl$blockingRead()
  expect_true('txtq' %in% names(out$otherPkgs))
})

test_that("the minion can login/logout", {

  carl <- Minion$new(c('dsSwissKnifeClient', 'dsBaseClient', 'dsQueryLibrary'))

  obj <- carl$bindUser('guest', 'guest123', config$loginData)
  carl$unbindUser()
  expect_equal(obj$message, 'opals')
})





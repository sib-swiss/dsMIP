test_that("minion initializes and queues are in place", {
  kevin <- Minion$new('guest',c('dsSwissKnifeClient'), '/datashield-engine')
  reqP <- kevin$reqQ$path()
  resP <- kevin$resQ$path()
  expect_true(file.exists(reqP))
  expect_true(file.exists(resP))
  kevin$stopQueues()
  expect_false(file.exists(reqP))
  expect_false(file.exists(resP))
})

test_that("head minion can start and stop the listener process", {
  carl <- HeadMinion$new('guest',c('dsSwissKnifeClient'), '/datashield-engine')
  carl$startProc()
  pid <- carl$getProc()$get_pid()
  x <- system(paste0('ps -p ', pid), intern = TRUE)
  expect_match(x[2], as.character(pid))
  carl$stopProc()
  expect_null(carl$getProc())
  suppressWarnings( x <- system(paste0('ps -p ', pid), intern = TRUE))
  expect_equal(attr(x, 'status'), 1)
  carl$stopQueues()
})

test_that("minion can load libraries via listener", {
  carl <- HeadMinion$new('guest',c('dsSwissKnifeClient'))
  expect_error(carl$startProc(), regexp = 'queue')
  carl$startQueues('/datashield-engine')
  carl$startProc()
  carl$loadLibs()
  x <- carl$sendRequest(func = sessionInfo)
  expect_true('dsSwissKnifeClient' %in% names(x$message$otherPkgs))
  m <- carl$stopProc()
  expect_match(m$message, 'Stopped')
  carl$stopQueues()
})

test_that("minion can log into and out of remote nodes", {
  carl <- HeadMinion$new('guest',c('dsSwissKnifeClient'))

  carl$startQueues('/datashield-engine')
  carl$startProc()
  carl$loadLibs()
  logged <- carl$login('guest123', config$loginData, TRUE)
  expect_equal(logged[['message']], 'opals')
  x <- carl$stopProc()
  expect_match(x$message, 'logged out')
  carl$stopQueues()
})


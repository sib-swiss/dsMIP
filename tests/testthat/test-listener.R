test_that("listener listens to basic commands", {
  resPath <- paste0(genPath, '/test.resq')
  resQ <- txtq(resPath)
  #### the message has this structure:
  # $resPath - path to the response queue
  #### no pid for now, try with one response queue per request # $pid - pid of the requesting process (to differentiate between 2 simultaneous requests from the same user)
  # $fun - the *name* of the function to be called (the function must be defined before)
  # $args - a list containing the function arguments
  # $waitForIt -  a flag indicating a result should be returned in the response queue
  ############
  mesg <- list(fun = 'get', args = list('opals'), resPath = resPath)
  reqQ$push('fun', jsonlite::serializeJSON(mesg))
  x <- resQ$pop()
  y <- jsonlite::unserializeJSON(x$message)
  reqQ$log()

})

test_that("head minion can start and stop the listener process", {
  x <- system(paste0('ps -p ', pid), intern = TRUE)
  expect_match(x[2], as.character(pid))
  carl$stopProc()
  expect_null(carl$getProc())
  suppressWarnings( x <- system(paste0('ps -p ', pid), intern = TRUE))
  carl <- HeadMinion$new('guest',c('dsSwissKnifeClient'), '/datashield-engine')
  carl$startProc()
  pid <- carl$getProc()$get_pid()
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


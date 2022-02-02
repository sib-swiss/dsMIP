carl <- Minion$new()
carl$bindUser('guest', 'guest123', config$loginData)




reqQ <- txtq(carl$getReqQueue())
resQ <- txtq(carl$getResQueue())
msg <- jsonlite::serializeJSON(list(func = fn, args = list(logindata=lg)))
fn <- function(logindata){
  opals <- datashield.login(logindata)
  assign('opals', opals, envir = .GlobalEnv)
}
reqQ$push('login', msg)
x <- resQ$pop()
x
jsonlite::unserializeJSON(x)
carl$userBind('guest', 'guest123', config$loginData)
lg <- config$loginData
lg$user <- 'guest'
lg$password <- 'guest123'
msg <- jsonlite::serializeJSON(list(func = ls))


userLogin <- function(user, pass){
  logindata <- get('logindata', envir = .GlobalEnv) # it's been put there at startup
  logindata$user <- user
  logindata$password <- pass
  userOpals <- datashield.login(logindata) # any error here will be escalated
  datashield.logout(userOpals) # don't need this sesssion, just the authentication
  return(TRUE)
}

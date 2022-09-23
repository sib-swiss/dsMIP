login <- function(usr, pwd){
  # config is in the global env
  logindata <- config$loginData
  resourceMap <- config$resourceMap

  # finalise logindata
  logindata$user <-config$appUser
  logindata$password <- Sys.getenv('pass')
  logindata$driver <- 'OpalDriver'
  Sys.unsetenv('pass')
  ######### make one logindata entry per resource (as opposed to one per server - we'll have one or more connections per server) ###########
  resnames <- dssSwapKeys(resourceMap)

  logindata <- lapply(names(resnames), function(x){

    out <- logindata[logindata$server == resnames[[x]],,drop=FALSE]
    out$server <- x
    out
  }) %>% Reduce(rbind,.)
  ##################################################
  ######################### login where allowed, fail silently elsewhere #################################
  opals <- list()
  for(i in logindata$server){
    try(opals[i] <- datashield.login(logindata[logindata$server == i,,drop = FALSE]), silent = FALSE)
    #opals[i] <- datashield.login(logindata[logindata$server == i,,drop = FALSE])
  }


  assign('opals', opals, envir = .GlobalEnv)
  return(names(opals))
}

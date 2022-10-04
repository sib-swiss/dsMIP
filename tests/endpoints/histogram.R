histogram = list(
  method = 'GET',
  FUN = function(req, res){
    params <- req$parameters_query
    if(!('var' %in% names(params))){
      stop('var is mandatory')
    }
    if(is.null(params$type)){
      params$type <- 'combine'
    }
    rPath <- paste0(resPath, '/', req$cookies[['user']], req$cookies[['sid']])
    out <- qCommand(reqQ, rPath, message = list(fun = 'sliceNdice', args = list('ds.histogram', var = params$var, type = params$type, cohorts = params$datasets)))
    if(out$title == 'result'){
      res$set_body(out$message)
    } else {
      stop(out$message)
    }
  }
)


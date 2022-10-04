runAlgorithm = list(
  method = 'POST',
  FUN = function(req, res){
    experiment <- jsonlite::fromJSON(req$body)
    rPath <- paste0(resPath, '/', req$cookies[['user']], req$cookies[['sid']])
    out <- qCommand(reqQ, rPath, message = list(fun = 'runAlgo', args = list(algoArgs = experiment)))
    if(out$title == 'result'){
      res$set_body(out$message)
    } else {
      stop(out$message)
    }
  }
)


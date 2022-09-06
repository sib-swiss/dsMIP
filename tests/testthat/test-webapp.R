
combinedHeatmap <- function (x = NULL, y = NULL,  show = "zoomed",
          numints = 20, method = "smallCellsRule", k = 3, noise = 0.25,
          cohorts = NULL) {

  for (var in c(x,y)){
    if(varmap[[var]]$type != 'number'){
      # nothing to do here:
      return(NULL)
    }
  }
  if(!is.null(cohorts)){
    cohorts <- strsplit(cohorts, ',\\s*')
  }

  finalCohorts <- Reduce(function(x,y){ # keep only the common datasources
    if(is.null(x)){
      return(y)
    } else if(is.null(y)){
      return(x)
    } else {
      return(intersect(x,y))
    }
  }, list(cohorts, varmap[[x]]$cohorts, varmap[[y]]$cohorts))


  type = 'combine'
  if (is.null(finalCohorts)) {
    datasources <- datashield.connections_find()
  } else {
    datasources <- opals[finalCohorts]
  }

   if (is.null(x)) {
    stop("x=NULL. Please provide the names of the 1st numeric vector!",
         call. = FALSE)
  }
  if (is.null(y)) {
    stop("y=NULL. Please provide the names of the 2nd numeric vector!",
         call. = FALSE)
  }
  x <- paste0('working_set$',x)
  y <- paste0('working_set$',y)

  if (method != "smallCellsRule" & method != "deterministic" &
      method != "probabilistic") {
    stop("Function argument \"method\" has to be either \"smallCellsRule\" or \"deterministic\" or \"probabilistic\"",
         call. = FALSE)
  }
  xnames <- extract(x)
  x.lab <- sub('working_set$', '', xnames[[length(xnames)]], fixed = TRUE)
  ynames <- extract(y)
  y.lab <- sub('working_set$', '',  ynames[[length(ynames)]], fixed = TRUE)
  stdnames <- names(datasources)
  num.sources <- length(datasources)
  if (method == "deterministic") {
    method.indicator <- 1
    cally <- paste0("heatmapPlotDS(", x, ",", y, ",", k,
                    ",", noise, ",", method.indicator, ")")
    anonymous.data <- DSI::datashield.aggregate(datasources,
                                                cally)
    pooled.points.x <- c()
    pooled.points.y <- c()
    for (i in 1:num.sources) {
      pooled.points.x[[i]] <- anonymous.data[[i]][[1]]
      pooled.points.y[[i]] <- anonymous.data[[i]][[2]]
    }
  }
  if (method == "probabilistic") {
    method.indicator <- 2
    cally <- paste0("heatmapPlotDS(", x, ",", y, ",", k,
                    ",", noise, ",", method.indicator, ")")
    anonymous.data <- DSI::datashield.aggregate(datasources,
                                                cally)
    pooled.points.x <- c()
    pooled.points.y <- c()
    for (i in 1:num.sources) {
      pooled.points.x[[i]] <- anonymous.data[[i]][[1]]
      pooled.points.y[[i]] <- anonymous.data[[i]][[2]]
    }
  }

    if (method == "smallCellsRule") {
      cally <- paste("rangeDS(", x, ")")
      x.ranges <- DSI::datashield.aggregate(datasources,
                                            as.symbol(cally))
      cally <- paste("rangeDS(", y, ")")
      y.ranges <- DSI::datashield.aggregate(datasources,
                                            as.symbol(cally))
      x.minrs <- c()
      x.maxrs <- c()
      y.minrs <- c()
      y.maxrs <- c()
      for (i in 1:num.sources) {
        x.minrs <- append(x.minrs, x.ranges[[i]][1])
        x.maxrs <- append(x.maxrs, x.ranges[[i]][2])
        y.minrs <- append(y.minrs, y.ranges[[i]][1])
        y.maxrs <- append(y.maxrs, y.ranges[[i]][2])
      }
      x.range.arg <- c(min(x.minrs), max(x.maxrs))
      y.range.arg <- c(min(y.minrs), max(y.maxrs))
      x.global.min <- x.range.arg[1]
      x.global.max <- x.range.arg[2]
      y.global.min <- y.range.arg[1]
      y.global.max <- y.range.arg[2]
      cally <- paste0("densityGridDS(", x, ",", y, ",",
                      limits = T, ",", x.global.min, ",", x.global.max,
                      ",", y.global.min, ",", y.global.max, ",", numints,
                      ")")
      grid.density.obj <- DSI::datashield.aggregate(datasources,
                                                    as.symbol(cally))
      numcol <- dim(grid.density.obj[[1]])[2]
      for (i in 1:num.sources) {
        message(stdnames[i], ": ", names(dimnames(grid.density.obj[[i]])[2]))
      }
      Global.grid.density <- matrix(0, dim(grid.density.obj[[1]])[1],
                                    numcol - 2)
      for (i in 1:num.sources) {
        Global.grid.density <- Global.grid.density +
          grid.density.obj[[i]][, 1:(numcol - 2)]
      }
    }
    else {
      if (method == "deterministic" | method == "probabilistic") {
        xvect <- unlist(pooled.points.x)
        yvect <- unlist(pooled.points.y)
        y.min <- min(yvect)
        x.min <- min(xvect)
        y.max <- max(yvect)
        x.max <- max(xvect)
        y.range <- y.max - y.min
        x.range <- x.max - x.min
        y.interval <- y.range/numints
        x.interval <- x.range/numints
        y.cuts <- seq(from = y.min, to = y.max, by = y.interval)
        y.mids <- seq(from = (y.min + y.interval/2),
                      to = (y.max - y.interval/2), by = y.interval)
        y.cuts[numints + 1] <- y.cuts[numints + 1] *
          1.001
        x.cuts <- seq(from = x.min, to = x.max, by = x.interval)
        x.mids <- seq(from = (x.min + x.interval/2),
                      to = (x.max - x.interval/2), by = x.interval)
        x.cuts[numints + 1] <- x.cuts[numints + 1] *
          1.001
        grid.density <- matrix(0, nrow = numints, ncol = numints)
        for (j in 1:numints) {
          for (k in 1:numints) {
            grid.density[j, k] <- sum(1 * (yvect >= y.cuts[k] &
                                             yvect < y.cuts[k + 1] & xvect >= x.cuts[j] &
                                             xvect < x.cuts[j + 1]), na.rm = TRUE)
          }
        }
        grid.density.obj <- list()
        grid.density.obj[[1]] <- cbind(grid.density,
                                       x.mids, y.mids)
        numcol <- dim(grid.density.obj[[1]])[2]
        Global.grid.density <- grid.density
      }
    }
    graphics::par(mfrow = c(1, 1))
    x <- grid.density.obj[[1]][, (numcol - 1)]
    y <- grid.density.obj[[1]][, (numcol)]
    z <- Global.grid.density
    if (show == "all") {
      png('./heatmap.png')
      fields::image.plot(x, y, z, xlab = x.lab, ylab = y.lab,
                         main = "Heatmap Plot of the Pooled Data")
      dev.off()
        x <- x  # to keep the code format
    }
    else if (show == "zoomed") {
      flag <- 0
      rows_top <- 1
      while (flag != 1) {
        if (all(Global.grid.density[rows_top, ] == 0)) {
          rows_top <- rows_top + 1
        }
        else {
          flag <- 1
        }
      }
      if (rows_top == 1) {
        dummy_top <- rows_top
      }
      else {
        dummy_top <- rows_top - 1
      }
      flag <- 0
      rows_bot <- dim(Global.grid.density)[1]
      while (flag != 1) {
        if (all(Global.grid.density[rows_bot, ] == 0)) {
          rows_bot <- rows_bot - 1
        }
        else {
          flag <- 1
        }
      }
      if (rows_bot == dim(Global.grid.density)[1]) {
        dummy_bot <- rows_bot
      }
      else {
        dummy_bot <- rows_bot + 1
      }
      flag <- 0
      col_left <- 1
      while (flag != 1) {
        if (all(Global.grid.density[, col_left] == 0)) {
          col_left <- col_left + 1
        }
        else {
          flag <- 1
        }
      }
      if (col_left == 1) {
        dummy_left <- col_left
      }
      else {
        dummy_left <- col_left - 1
      }
      flag <- 0
      col_right <- dim(Global.grid.density)[2]
      while (flag != 1) {
        if (all(Global.grid.density[, col_right] == 0)) {
          col_right <- col_right - 1
        }
        else {
          flag <- 1
        }
      }
      if (col_right == 1) {
        dummy_right <- dim(Global.grid.density)[2]
      }
      else {
        dummy_right <- col_right + 1
      }
      z.zoomed <- Global.grid.density[dummy_top:dummy_bot,
                                      dummy_left:dummy_right]
      x.zoomed <- x[dummy_top:dummy_bot]
      y.zoomed <- y[dummy_left:dummy_right]
      x <- x.zoomed
      y <- y.zoomed
    }
    else {
      stop("Function argument \"show\" has to be either \"all\" or \"zoomed\"")
    }
   return(list(Global.grid.density, xlab = x.lab, ylab = y.lab, x = x, y = y))
}





######################################:::::::::::::::::::::::::::================================

freePipes <- function(pipeDir, maxWorkers, timeout){
  resPipes <- grep('res', dir(pipeDir), value = TRUE) # the workers have each their '.res' pipe
  npipes <- length(resPipes)
  found <- TRUE
  if(npipes >= maxWorkers){
    found <- FALSE
    for(f in resPipes){
      fRes <- paste0(pipeDir, '/', f)
      pRes <- txtq(fRes)
      lastM <- pRes$log()
      lastM <- lastM[nrow(lastM), c('title', 'time')]
      print(lastM)
      if(is.na(lastM$time) ||
         length(lastM$time) == 0 ||
         (as.numeric(Sys.time() - as.POSIXct(lastM$time)) < timeout && lastM$title !='timeout')){
        next
      }
      # if we are here we need to do some cleaning, first identify the req pipe as well
      fReq <- sub('\\.res', '.req', fRes)
      pReq <- txtq(fReq)
      lastR <- pReq$log()
      lastR <- lastR[nrow(lastR), c('title', 'time')]
      print(lastR)
      if(lastM$title == 'timeout' ||
         (
           !is.na(lastR$title) && length(lastR$title) > 0 && lastR$title == 'STOP'
          ) ){ # the listener is no longer with us, get rid of the pipes
        pReq$destroy()
        pRes$destroy()
        found <- TRUE
        next
      }
      if(as.numeric(Sys.time() - as.POSIXct(lastM$time)) >= timeout ){
        # first check if it's not working on something, that is if we have a more recent request
        if(is.na(lastR$time) ||
           length(lastR$time) == 0 ||
           as.POSIXct(lastR$time) < as.POSIXct(lastM$time)){ # probably nothing going on
          # send a stop message, if the listener is dead, next time the queues will be destroyed
          pReq$push('STOP', 'STOP')
          found <- TRUE
        }
      }
    }
  }
  return(found)
}

applyFunc <- function(func, var, type , cohorts = NULL){
  op <- opals
  # only use the cohorts where we know we have this variable
  # varmap is a global list in the forked process
  if(!is.null(varmap[[var]]$cohorts)){
    if(is.null(cohorts)){
      cohorts <- varmap[[var]]$cohorts
    } else {
      cohorts <- strsplit(cohorts, ',\\s*')[[1]]
      cohorts <- intersect(cohorts, varmap[[var]]$cohorts)
    }
  }

  op <-opals[cohorts]

  if(varmap[[var]]$type == 'number'){
    var = paste0('working_set$', var)
    ret <- do.call(func, list(var,type = type ,datasources = op))
    if(type == 'split'){
      if(length(names(op)) == 1){
        ret <- list(ret)
      }
      names(ret) <- names(op)
    } else {
      ret <- list(global = ret)
    }

  } else if(varmap[[var]]$type == 'nominal'){
    var = paste0('working_set$', var)
    ret <- ds.table1D(var,type = type , warningMessage = FALSE, datasources = op)$counts
    if(!is.list(ret)){
      ret <- list(global = ret)
    }
    ret <- sapply(ret, function(x){
      q <- list()
      q[dimnames(x)[[1]]] <- x[,1]
      q
    }, simplify = FALSE)


  } else {
    stop(paste0('Not implemented for type ',varmap[[var]]$type ))
  }
  ret
}



runAlgo <- function(algoArgs){
  # maybe one day I'll take these functions out and in the listener at start
  completeCases <- function(cols, df = 'working_set', output = 'cc'){ # subset to the necessary columns only, then complete.cases
    filterCols <- paste(cols, collapse = "','") %>% paste0("c('",., "')")
    dssSubset(output, df, row.filter = paste0('complete.cases(',df, '[,', filterCols, '])'), col.filter = filterCols)
    return(output)
  }
  parseRegressionArgs <- function(){
    var <- algoArgs$algorithm$variable # should be only one
    coVars <-  algoArgs$algorithm$coVariables
    datasets <- Reduce(function(x,y){
                                        list(cohorts = intersect(x$cohorts, y$cohorts))
                             }, varmap[c(coVars, var)]) # consider only the datasources that contain all these coVariables

    if(!is.null(algoArgs$datasets)){ # then intersect them with the required ones:
      datasets <- intersect(datasets$cohorts, algoArgs$datasets)
    }
    if(length(datasets) == 0){
      stop('No datasets found for these coVariables.')
    }

    textVars <- paste(coVars, collapse = ' + ')
    formula <- paste0(var, ' ~ ', textVars)

    return(list(formula = formula, datasources = opals[datasets], data = completeCases(c(var,coVars)))) # completeCases has side effects!

  }

   parseLogisticArgs <- function(){
     genArgs <-parseRegressionArgs()
     pos.level <- algoArgs$algorithm[['pos-level']]
     dssDeriveColumn('cc', col.name = algoArgs$algorithm$variable, formula = paste0('one.versus.others(',algoArgs$algorithm$variable, ',"', pos.level, '")', datasources = genArgs$datasoruces)) # make the covariate binary
     return(genArgs)
   }


  formatters <- list('linear-regression' = parseRegressionArgs,
                     'logistic-regression' = parseLogisticArgs
                    )

  algos <- list('linear-regression' = list(ds.glm,
                                          family = 'gaussian'
                                        ),
                'logistic-regression' = list(ds.glm,
                                           family = 'binomial'
                                        )
                )


  paramList <- formatters[[algoArgs$algorithm$id]]()
  res <- eval(as.call(c(algos[[algoArgs$algorithm$id]], paramList)))
  res$family <- NULL # jsonlite doesn't like family
  if('coefficients' %in% names(res)){
    res$coefficients <- as.data.frame(res$coefficients) # to keep the names after to/from json
  }
  try(datashield.rm(paramList$datasources, paramList$data), silent = TRUE) # keep the remote sessions slim
  res
}

sentry <- function(user , password ){ # must return a head minion
  if(!freePipes(paste0(tempdir(TRUE), '/',config$dir), config$workers, 60)){
    stop("Too many connections")
  }
  carl <- HeadMinion$new(user, config$loginData, config$resourceMap,  config$libraries)
  carl$startQueues(config$dir)
  carl$startProc()
  carl$loadLibs()
  logged <- carl$login(password, TRUE)

  if(logged$title == 'error'){
    stop(logged$message)
  }
  devSetOptions <- function(){ # only in dev
    dssSetOption(list('cdm_schema' = 'synthea_omop'))
    dssSetOption(list('vocabulary_schema' = 'omop_vocabulary'))
  }
  carl$sendRequest(devSetOptions, waitForIt = FALSE)

  return(carl)
}


sentryBackend <- SentryBackend$new(folder = config$dir, FUN = sentry)

sentryMw <- AuthMiddleware$new(
  auth_backend = sentryBackend,
  routes = "/",
  match = "partial",
  id = "sentry_middleware"
)

app <-  Application$new(content_type = "application/json",middleware = list(sentryMw))

app$add_get(
  path = "/login",
  FUN = function(req,res){
    bob <- req$cookies$minion
    req$cookies$minion <- NULL
    remoteLoad <- function(resourceMap, dfs){
      sapply(resourceMap$server, function(res){
        datashield.assign.resource(opals[res], sub('.','_',res, fixed = TRUE), res, async = FALSE)
      }) # resources are in
      # now dfs:
      sapply(dfs, function(x){
        where_clause <- NULL
        if(x == 'measurement'){
          where_clause <- 'value_as_number is not null'
        }
        dsqLoad(symbol= x, domain = 'concept_name', query_name = x, where_clause = where_clause, union = TRUE, datasources = opals)
      }) # done with the data frames
    } # remoteLoad
    nodeRes <- bob$getNodeResources()
    bob$sendRequest(remoteLoad, args = list(resourceMap = nodeRes, dfs = config$mainGroups), waitForIt = FALSE)

    prepareData <- function(){
      dssPivot(symbol = 'wide_m', what ='measurement', value.var = 'value_as_number',
               formula = 'person_id ~ measurement_name',
               by.col = 'person_id',
               fun.aggregate = function(x)x[1],
               datasources = opals)
      dssJoin(what = c('wide_m', 'person'),
              symbol = 'working_set',
              by = 'person_id',
              datasources = opals)
      n <- dssColNames('working_set')
      sapply(names(n), function(x){
        cnames <- n[[x]]
        cnames <- sub('measurement_name.', '', cnames, fixed = TRUE)
        dssColNames('working_set', cnames, datasources = opals[[x]])
      })
      ds.summary('working_set')

    }
    # launch the widening and join (async)
    bob$sendRequest(prepareData, waitForIt = FALSE)
    res$set_body('OK')
  })

app$add_get(
  path = "/getvars",
  FUN = function(req,res){
    cacheDir <- paste0(tempdir(check = TRUE), '/cache')
    if(!dir.exists(cacheDir)){
      dir.create(cacheDir)
    }
    nocache <- NULL
    if('nocache' %in% names(req$parameters_query)){
      nocache <- req$parameters_query[['nocache']] %>% tolower
    }

    if(!is.null(nocache) && nocache %in% c('true', 'yes')){
      cache <- FALSE
    } else {
      cache <- TRUE
    }
    # retrieve the minion:
    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]

    bob <- Minion$new(myUser, config$loginData, config$resourceMap)

    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    bob$reqQ <- txtq(paste0(qPath, '.req'))
    bob$resQ <- txtq(paste0(qPath, '.res'))


 if(!cache || !file.exists(paste0(cacheDir,'/bubble.json'))){
      # we have to do the work here.
    bubbleData <- list()
#    bubbleData$datasets <- lapply(config$loginData$server,function(x){
#      list(id = x, label  = x)
#    })
    bubbleData$datasets <- lapply(names(dssSwapKeys(config$resourceMap)),function(x){
            list(id = x, label  = x)
          })


    bubbleData$rootGroup <- list(id = 'root', label = 'Root Group', groups = c('person', 'measurement'))


    getVars <- function(grps){

      vars <- list(list(id ='date_of_birth', type = 'character'), list(id='gender', type = 'nominal'),
                   list(id = 'race', type = 'nominal'), list(id ='ethnicity', type = 'nominal'))
      grps <- setdiff(grps, 'person')
      p <- lapply(vars, function(var) var$id)
      vars_done <- c()


      grps <- sapply(grps, function(x){
        sapply(ds.levels(paste0(x, '$', x, '_name'), datasources = opals), function(y){
          make.names(y$Levels)
        }, simplify = FALSE)
      }, simplify = FALSE)

      varmap <- sapply(grps, dssSwapKeys, simplify = FALSE)
      varmap <- sapply(varmap, function(x) {

        sapply(x, function(y){
          list(type = 'number', cohorts = y)
        }, simplify = FALSE)

      }, simplify = FALSE)

      varmap <- Reduce(c, varmap) # keep only colname->cohorts (not dfname->colname->cohorts)
      person_cols <- ds.colnames('person', datasources = opals) %>% dssSwapKeys()
      person_cols <- person_cols[unlist(p)]

      varmap <- c(varmap, sapply(person_cols, function(x){   # add the person coVariables (all nominal)
        list(cohorts = x, type = 'nominal')
      }, simplify = FALSE))


      ################
      for (grp in grps) {
        for (db in grp) {
          for (var in db) {
            # Check if it's already on the list before adding it
            if (!(var %in% vars_done)) {
              vars_done <- append(vars_done, var) # Append var to the done list
              vars <- append(vars, list(list(id = var, type = "number")))
            }
          }
        }
      }

      grps$demographics <- p

      list(groups = grps, varmap = varmap, vars = vars)
    } # getvars

# debug:
   assign('kevin', bob, envir = .GlobalEnv)
    result <- bob$sendRequest(getVars, list(grps = config$mainGroups), timeout = 120)
    if(result$title == 'error'){
      stop(result$message)
    }

    bubbleData$coVariables <- result$message$var
    bubbleData$groups <- lapply(names(result$message$groups), function(x){
      list(id = x, label = x, coVariables =  result$message$groups[[x]])
    })
  #  bubbleData$groups[[length(bubbleData$groups)+1]] <- list(id = 'cohorts', label = 'Cohorts', coVariables = names(dssSwapKeys(config$resourceMap)) %>%  sub('\\..*','',.)) # without the 'db suffix', function(x){
    jsonBubble <- jsonlite::toJSON(bubbleData)
    jsonVarMap <- jsonlite::toJSON(result$message$varmap)
    save(jsonBubble, file = paste0(cacheDir, '/bubble.json'))
    save(jsonVarMap, file = paste0(cacheDir, '/varmap.json'))
 } else {
   load(file = paste0(cacheDir, '/bubble.json'))
   load(file = paste0(cacheDir, '/varmap.json'))
 }
    setvarmap <- function(x){
      assign('varmap', x, envir = .GlobalEnv)
    }
    varmap <- jsonlite::fromJSON(jsonVarMap)
    bob$sendRequest(setvarmap, list(varmap), waitForIt = FALSE)
    x <- jsonlite::fromJSON(jsonBubble, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
    jsonBubble <- jsonlite::toJSON(x, auto_unbox = TRUE)
    res$set_body(jsonBubble)
  }
)


app$add_get(
  path = "/quantiles",
  FUN = function(req,res){

    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]

    kevin <- Minion$new(myUser, config$loginData, config$resourceMap)

    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    kevin$reqQ <- txtq(paste0(qPath, '.req'))
    kevin$resQ <- txtq(paste0(qPath, '.res'))

    params <- req$parameters_query
    if(!('var' %in% names(params))){
      stop('var is mandatory')
    }
    if(is.null(params$type)){
      params$type <- 'combine'
    }

    r<- kevin$sendRequest(applyFunc, list('ds.quantileMean', params$var, params$type, params$cohorts))

    res$set_body(jsonlite::toJSON(r$message, auto_unbox = TRUE))

})


app$add_get(
  path = "/descStats",
  FUN = function(req,res){

    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]

    kevin <- Minion$new(myUser, config$loginData, config$resourceMap)

    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    kevin$reqQ <- txtq(paste0(qPath, '.req'))
    kevin$resQ <- txtq(paste0(qPath, '.res'))

    params <- req$parameters_query
    covars <- params$covariables
    vars <- params$variables

    quants <- sapply(c(covars,vars), function(var) kevin$sendRequest(applyFunc, list('ds.quantileMean', var, 'combine', params$cohorts)), simplify = FALSE)
    heatmaps <- lapply(vars, function(v) lapply(covars, function(c) kevin$sendRequest(combinedHeatmap, list(x = c, y = v, cohorts = params$cohorts))))

    res$set_body(jsonlite::toJSON(list(quants = sapply(quants, '[[', 'message'), heatmaps = lapply(heatmaps, function(x) lapply(x, '[[', 'message'))), auto_unbox = TRUE))

  })








app$add_get(
  path = "/histogram",
  FUN = function(req,res){

    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]
    kevin <- Minion$new(myUser, config$loginData, config$resourceMap)

    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    kevin$reqQ <- txtq(paste0(qPath, '.req'))
    kevin$resQ <- txtq(paste0(qPath, '.res'))

    params <- req$parameters_query
    if(!('var' %in% names(params))){
      stop('var is mandatory')
    }
    if(is.null(params$type)){
      params$type <- 'combine'
    }

  #  r <- kevin$sendRequest(f, list(params$var, params$type, params$cohorts))
    r <- kevin$sendRequest(applyFunc, list('ds.histogram', params$var, params$type, params$cohorts))
  #  if(params$type == 'split'){
    if(r$title == 'result'){
      out <- sapply(r$message, unclass, simplify = FALSE)
    } else{
  #    class(r$message) <-  'list'
   #   out <- r$message
      out <- r$message
   }
    res$set_body(jsonlite::toJSON(out, auto_unbox = TRUE))

  })


app$add_get(
  path = "/logout",
  FUN = function(req,res){

    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]

    kevin <- Minion$new(myUser, config$loginData, config$resourceMap)
    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    kevin$reqQ <- txtq(paste0(qPath, '.req'))
    kevin$resQ <- txtq(paste0(qPath, '.res'))
    ret <- kevin$stopProc()
    kevin$stopQueues()
    res$set_body(jsonlite::toJSON(ret, auto_unbox = TRUE))
  })


app$add_post(
  path = "/runAlgorithm",
  match = "exact",
  FUN = function(req, res) {
    mySid <- req$cookies[['sid']]
    myUser <- req$cookies[['user']]
    stuart <- Minion$new(myUser, config$loginData, config$resourceMap)
    qPath <- paste0(tempdir(TRUE), '/', config$dir , '/',myUser, '_', mySid)
    stuart$reqQ <- txtq(paste0(qPath, '.req'))
    stuart$resQ <- txtq(paste0(qPath, '.res'))
    experiment <- jsonlite::fromJSON(req$body)
    model <- stuart$sendRequest(runAlgo, list(experiment))
    #res$set_body(jsonlite::toJSON(model$message, auto_unbox = TRUE))
    res$set_body(model$message)

  }
)



system.time(test_that("Login works", {
  ### make the request:

   credentials <- jsonlite::base64_enc("guest:guest123")
  headers <- list("Authorization" = sprintf("Basic %s", credentials))
  req <<- Request$new(
    path = "/login",
    parameters_query = list(nocache = 'true'),
    headers = headers
  )
  response <<- app$process_request(req)
  ck <<- list(user =  response$cookies$user$value, sid = response$cookies$sid$value)
  x <<- response$body
  expect_equal(x, 'OK')
}))

test_that(" Endpoint /getvars works", {


  ### make the request:
  req2 <- Request$new(
    path = "/getvars",
    parameters_query = list(nocache = 'true'),
    cookies = ck
  )
  response2 <- app$process_request(req2)
  x <<- jsonlite::fromJSON(response2$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(x$datasets[[1]]$id, 'omop_test.db')
})

test_that(" Endpoint /histogram works", {
  ### make the request:
  req2 <- Request$new(
    path = "/histogram",
    parameters_query = list(var = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", type = 'split', cohorts ="sophia.db"),
    cookies = ck
  )
  response3 <- app$process_request(req2)

  xxx<<- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(xxx), c(  "sophia.db"  ))
})

test_that(" Endpoint /histogram works for factors", {
  ### make the request:
  req2 <- Request$new(
    path = "/histogram",
    parameters_query = list(var = "ethnicity", type = 'split', cohorts ="sophia.db, test.db"),
    cookies = ck
  )
  response3 <- app$process_request(req2)

  xxx<<- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(xxx), c(  "sophia.db" , 'test.db' ))
})



test_that(" Endpoint /quantiles works", {
  ### make the request:
  req2 <- Request$new(
    path = "/quantiles",
    parameters_query = list(var = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", type = 'combine'),
    cookies = ck
  )
  response3 <- app$process_request(req2)
  x<- jsonlite::fromJSON(response3$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(length(x$global), 8)
})

test_that(" Endpoint /quantiles works for factors", {
  ### make the request:
  req <- Request$new(
    path = "/quantiles",
    parameters_query = list(var = "ethnicity", type = 'combine', cohorts = 'sophia.db'),
    cookies = ck
  )
  response <- app$process_request(req)
  x<- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(x), c('global' ))
})


test_that(" Endpoint /runAlgorithm works with linear regression", {
  ### make the request:
  req3 <- Request$new(
    path = "/runAlgorithm",

    body = jsonlite::toJSON(list(
                            algorithm = list(id = 'linear-regression',
                                             variable = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma",
                                             coVariables = c("Urea.nitrogen..Mass.volume..in.Serum.or.Plasma" , "Albumin..Mass.volume..in.Serum.or.Plasma")),
                            datasets =c("sophia.db"))),
    method = 'POST',
    cookies = ck,
    content_type = 'application/json'
  )
  response3 <- app$process_request(req3)


  expect_equal(response3$body$Ntotal, 107)
})

test_that(" Endpoint /runAlgorithm works with logistic regression", {
  ### make the request:
  req4 <- Request$new(
    path = "/runAlgorithm",

    body = jsonlite::toJSON(list(algorithm = list(id = 'logistic-regression',
                                                  coVariables = c("Urea.nitrogen..Mass.volume..in.Serum.or.Plasma" , "Albumin..Mass.volume..in.Serum.or.Plasma"),
                                                  variable = "race",
                                                  'pos-level' = 'White'),
                                 datasets =c("sophia.db", 'test.db'))),
    method = 'POST',
    cookies = ck,
    content_type = 'application/json'
  )
  response4 <- app$process_request(req4)

  expect_equal(response4$body$Ntotal, 214)
})

test_that(" Endpoint /descStats works ", {
  ### make the request:
  req <- Request$new(
    path = "/descStats",
    parameters_query = list(covariables = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", variables ="Urea.nitrogen..Mass.volume..in.Serum.or.Plasma"),
    cookies = ck
  )
  response <- app$process_request(req)
  xx<<- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = TRUE)
  expect_equal(length(xx), 2)
})


#test_that(" Endpoint /logout works", {
  ### make the request:
#  req2 <- Request$new(
#    path = "/logout",
#    cookies = ck
#  )
#  response4 <- app$process_request(req2)
#  xx<<- jsonlite::fromJSON(response4$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
#  expect_equal(xx[['title']], c('STOP' ))
#})





#x <- kevin$sendRequest(applyFunc, list('ds.histogram',"Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", type = 'split', cohorts ="sophia.db"))$message
#x <- kevin$sendRequest(applyFunc, list(ds.quantileMean,"Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", type = 'combine', cohorts ="sophia.db, test.db"))$message

f <- function(vars){
#dssColNames('working_set')
  #ls(envir = .GlobalEnv)
  Reduce(function(x,y){
    list(cohorts = intersect(x$cohorts, y$cohorts))
  }, varmap[vars])

}

gv <- function(f,...){
  #dssDeriveColumn('cc', col.name = 'race', formula = 'one.versus.others(race, "White")') # make the covariate binary
 # ds.levels('cc$race')
 # datashield.symbols(opals)
  return(list(cols = dssColNames('working_set'), varmap = varmap))
  png('/tmp/heatmap')

  out <- f(...)
  dev.off()
  return(out)
}
vm <- kevin$sendRequest(gv,timeout = 180)

x <- kevin$sendRequest(gv, list(combinedHeatmap,x="working_set$Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", y="working_set$Urea.nitrogen..Mass.volume..in.Serum.or.Plasma"))
x <- kevin$sendRequest(gv, list(combinedHeatmap,x="working_set$ethnicity", y="working_set$Urea.nitrogen..Mass.volume..in.Serum.or.Plasma"))
o <- c()
for (i in 1:length(x$message$y)){
  o <- c(o, x$message$y[i] - x$message$y[i-1])
}


Reduce(function(x,y){
  if(is.null(x)){
    return(y)
  } else if(is.null(y)){
    return(x)
  } else {
    return(intersect(x,y))
  }
}, list(NULL, NULL
        ,NULL ))

covars = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma"
vars ="Urea.nitrogen..Mass.volume..in.Serum.or.Plasma"

sapply(c(covars,vars), function(var) kevin$sendRequest(applyFunc, list('ds.quantileMean', var, 'combine', NULL)))

resnames <- dssSwapKeys(config$resourceMap)

logindata2 <- lapply(names(resnames), function(x){
  out <- logindata[logindata2$server == resnames[[x]],,drop=FALSE]
  out$server <- x
  out
}) %>% Reduce(rbind,.)

runAlgo <- function(algoArgs){


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
    op <- opals[datasets]
    textVars <- paste(coVars, collapse = ' + ')
    formula <- paste0(var, ' ~ ', textVars)
    ############## important for all functions!!!! #################
    dfName <- restrictToRelevantCols(c(var,coVars), 'working_set', op)

    ##################################################################

    return(list(formula = formula, datasources = op, data = dfName)) #  restrictToRelevantCols has side effects!

  }

  parseLogisticArgs <- function(){
    genArgs <-parseRegressionArgs()
    pos.level <- algoArgs$algorithm[['pos-level']]
    dssDeriveColumn(genArgs$data, col.name = algoArgs$algorithm$variable, formula = paste0('one.versus.others(',algoArgs$algorithm$variable, ',"', pos.level, '")'), datasources = genArgs$datasources) # make the covariate binary
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


###############
  paramList <- formatters[[algoArgs$algorithm$id]]()
  ############## defining it here for the main function
 # on.exit(
#    try(datashield.rm(op, paramList$data)),
 #   add = TRUE, after = FALSE
#  )
  ##################
  res <- eval(as.call(c(algos[[algoArgs$algorithm$id]], paramList)))
  res$family <- NULL # jsonlite doesn't like family
  if('coefficients' %in% names(res)){
    res$coefficients <- as.data.frame(res$coefficients) # to keep the names after to/from json
  }
  res
}

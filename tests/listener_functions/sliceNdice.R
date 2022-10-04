sliceNdice <- function(func, var, type = 'combine' , cohorts = NULL){
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
  } else {
    stop('This variable is not available in any cohort.')
  }

  op <-opals[cohorts]
############## important for all functions!!!! #################
  dfName <- restrictToRelevantCols(var, 'working_set', op)
  on.exit(
    try(datashield.rm(op, dfName)),
    add = TRUE, after = FALSE
  )
##################################################################
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
    cls <- sapply(ret, class) %>% Reduce(union, .)
    if('histogram' %in% cls){  # toJSON doesnt like this
      ret <- sapply(ret, unclass, simplify = FALSE)
    }

  } else if(varmap[[var]]$type == 'nominal'){
    var = paste0('working_set$', var)
    ret <- dssTable(var, type = type, datasources = op)

  ret <- sapply(ret, function(x){
      t <- sum(x)
      y <- as.list(x)
      y$Total <- t
      y
    }, simplify = FALSE)
  } else {
    stop(paste0('Not implemented for type ',varmap[[var]]$type ))
  }
  ret
}

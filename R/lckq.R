# modified txtq to allow locking
#' @import txtq
#' @export
LockingQ <- R6::R6Class('LockingQ',
                             inherit = R6_txtq,
                             public = list(
                               lockFor = function(code, timeout = 0){
                                on.exit(if(!is.null(lock)) filelock::unlock(lock))
                                lock <- filelock::lock(file.path(super$path(), "lock"), timeout = timeout) # ...
                                if(is.null(lock)){
                                  return(FALSE)
                                } else {
                                  force(code)
                                  return(TRUE)
                                }
                               }
                             )
)


# modified txtq to allow locking
#' @import txtq
#' @export
LockingQ <- R6::R6Class('LockingQ',
                             inherit = R6_txtq,
                              public = list(
                               initialize = function(path) {
                                 super$initialize(path, TRUE)
                               },
                               lockFor = function(code, timeout = 0){
                                on.exit(if(!is.null(lock)) filelock::unlock(lock))
                                lock <- filelock::lock(file.path(self$path(), "readlock"), timeout = timeout) # ...
                                if(is.null(lock)){
                                  return(FALSE)
                                } else {
                                  force(code)
                                  return(TRUE)
                                }
                               },
                               blockingRead = function(timeout = 60, every = 1){
                                st <- as.numeric(Sys.time())
                                while(TRUE){
                                   msg <-self$pop(1)
                                   if(nrow(msg) == 0){
                                     if(as.numeric(Sys.time()) - st > timeout){
                                       stop(paste0('Timeout (', timeout, ') while waiting to read from ', self$path()))
                                     }
                                     Sys.sleep(every)
                                     next
                                   }
                                   # no response queue cleaning, that's handled by the listener
                                   return(list(title = msg$title, message = msg$message, time = msg$time))
                                 } ## while true
                               } ### blockingRead

                             )
)

#' @export
lckq <- function(path) {
  LockingQ$new(path = path)
}



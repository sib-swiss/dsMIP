library(jsonlite)
library(magrittr)


stopThem <<- TRUE
dir.create(paste0(tempdir(check = TRUE), '/cache'))
x <- system2('docker', args = c('ps'), stdout = TRUE )
if(length(grep('docker_nodes_mip',x)) < 6){
  system2('docker-compose', args = c('-f', '../docker_nodes_mip/docker-compose.yml', 'up', '-d'))
  Sys.sleep(120)
  x <- system2('docker', args = c('ps'), stdout = TRUE )
  stopThem <<- TRUE
}
confFile <- '../config.json'
config <- readChar(confFile, file.info(confFile)$size) %>%
  gsub('(?<=:)\\s+|(?<=\\{)\\s+|(?<=,)\\s+|(?<=\\[)\\s+','',., perl = TRUE) %>%
  fromJSON()
if(stopThem){
  conts <<- lapply(x, function(y){
    out <- strsplit(y, '\\s+')[[1]]
    out[length(out)]
  }) %>% unlist
  conts[1] <- 'stop' # conts now contains the args for a 'docker stop' command - will be executed in teardown
}



getvars = list(
  method = 'GET',
  FUN = function(req, res){
        # bubbleJSON is set in .globalEnv at startup
          res$set_body(bubbleJSON)
  }
)


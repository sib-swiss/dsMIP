test_that("Login works", {
  ### make the request:

  credentials <- jsonlite::base64_enc("guest:guest123")
  headers <- list("Authorization" = sprintf("Basic %s", credentials))
  req <<- Request$new(
    path = "/login",
    headers = headers
  )
  response <- app$process_request(req)
  ck <<- list(user =  response$cookies$user$value, sid = response$cookies$sid$value)
  x <- response$body
  expect_equal(x, 'OK')
})


test_that(" Endpoint /getvars works", {


  ### make the request:
  req <- Request$new(
    path = "/getvars",
    parameters_query = list(nocache = 'true'),
    cookies = ck
  )
  response <- app$process_request(req)
  x <- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_identical(x,jsonlite::fromJSON(bubbleJSON, simplifyDataFrame = FALSE, simplifyMatrix = FALSE) )
})

test_that(" Endpoint /histogram works", {
  ### make the request:
  req <- Request$new(
    path = "/histogram",
    parameters_query = list(var = "Alanine.aminotransferase..Enzymatic.activity.volume..in.Serum.or.Plasma", datasets ="sophia.db"),
    cookies = ck
  )
  response <- app$process_request(req)

  x <- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(names(x), c(  "global"  ))
})

test_that(" Endpoint /histogram works for factors", {
  ### make the request:
  req <- Request$new(
    path = "/histogram",
    parameters_query = list(var = "ethnicity", cohorts ="sophia.db, test.db"),
    cookies = ck
  )
  response <- app$process_request(req)

  x<- jsonlite::fromJSON(response$body, simplifyDataFrame = FALSE, simplifyMatrix = FALSE)
  expect_equal(x$global$Total, 1838)
})

test_that(" Endpoint /runAlgorithm works with linear regression", {
  ### make the request:
  req <- Request$new(
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
  response <- app$process_request(req)
  x <- jsonlite::fromJSON(response$body)

  expect_equal(x$Ntotal, 107)
})

test_that(" Endpoint /runAlgorithm works with logistic regression", {
  ### make the request:
  req <- Request$new(
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
  response <- app$process_request(req)
  x <- jsonlite::fromJSON(response$body)
  expect_equal(x$Ntotal, 214)
})

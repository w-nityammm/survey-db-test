library(DBI)
library(RPostgres)
library(nwfscSurvey)
library(dplyr)
library(tidyr)
library(lubridate)

connectToDB <- function() {
  tryCatch({
    con <- dbConnect(
      Postgres(),
      dbname = "Trawl survey",
      host = "localhost",
      port = 5432,
      user = "postgres",
      password = ""
    )
    message("Successfully connected to the database!")
    return(con)
  },
  error = function(e) {
    message("Failed to connect to the database: ", e$message)
    return(NULL)
  })
}

con <- connectToDB()
if (is.null(con)) {
  stop("Database connection failed.")
}

checkTables <- function(con) {
  tables <- dbListTables(con)
  required_tables <- c("survey", "haul", "species", "catch")

   if (all(required_tables %in% tolower(tables))) {
    message("All required tables exist!")
    return(TRUE)
  } else {
    message("Missing tables detected.")
    return(FALSE)
  }
}

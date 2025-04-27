library(DBI)
library(RPostgres)
library(nwfscSurvey)
library(dplyr)
library(tidyr)
library(lubridate)

db_password <- Sys.getenv("DB_PASSWORD")

connectToDB <- function() {
  tryCatch({
    con <- dbConnect(
      Postgres(),
      dbname = "Trawl survey",
      host = "localhost",
      port = 5432,
      user = "postgres",
      password = db_password
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

load_nwfsc <- function() {
  message("Loading survey data from nwfscSurvey package...")

  haul_data <- pull_haul(survey = "NWFSC.Slope")
  catch_data <- pull_catch(survey = "NWFSC.Slope")

  message("Data loaded successfully!")
  return(list(haul = haul_data, catch = catch_data))
}

nwfsc_data <- tryCatch({
  load_nwfsc()
}, error = function(e) {
  message("Error loading nwfscSurvey data: ", e$message)
  return(NULL)
})

transform_nwfsc_data <- function(nwfsc_data) {
  if (is.null(nwfsc_data)) {
    stop("No data to transform.")
  }

  haul_raw <- nwfsc_data$haul
  catch_raw <- nwfsc_data$catch

  names(haul_raw) <- tolower(names(haul_raw))
  names(catch_raw) <- tolower(names(catch_raw))

  survey_data <- data.frame(
    survey_id = 1, # Need to decide a naming convention for this
    survey_name = "NWFSC.Slope",
    region = "nwfsc",
    start_date = min(haul_raw$date_formatted),
    end_date = max(haul_raw$date_formatted)
  )

  haul_data <- haul_raw %>%
    dplyr::select(
      trawl_id,
      date = date_formatted,
      pass,
      vessel,
      lat_start = vessel_start_latitude_dd,
      lon_start = vessel_start_longitude_dd,
      lat_end = vessel_end_latitude_dd,
      lon_end = vessel_end_longitude_dd,
      depth_m = depth_hi_prec_m,
      effort = area_swept_ha_der,
      performance,
      bottom_temp_c = temperature_at_gear_c_der
    )

  haul_data$effort_units <- "ha"

  unique_species <- catch_raw %>%
    distinct(scientific_name)

  spp <- unique_species

  get_itis <- function(spp) {
    tryCatch({
      Sys.sleep(0.2)
      out <- taxize::get_tsn(spp, accepted = FALSE, verbose = FALSE)
      if (is.na(out[1]) || length(out) == 0) {
        return(NA_integer_)
      }
      return(as.integer(out[1]))
    }, error = function(e) {
      message("Error retrieving ITIS code for ", spp, ": ", e$message)
      return(NA_integer_)
    })
  }

  species_data <- unique_species
  species_data$species_id <- NA_integer_

  for (i in 1:nrow(species_data)) {
    species_name <- species_data$scientific_name[i]
    message("Processing species ", i, " of ", nrow(species_data), ": ", species_name)
    species_data$species_id[i] <- get_itis(species_name)
  }

}

transformed_data <- transform_nwfsc_data(nwfsc_data)


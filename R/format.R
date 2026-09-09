#' a merge that preserves row and column order
#'
#' shamelessly stolen from https://stackoverflow.com/questions/17878048/merge-two-data-frames-while-keeping-the-original-row-order
#' @param x the "left" dataframe (all.x=TRUE)
#' @param y the "right" dataframe
#' @param ... Additional arguments passed on to methgods
#' @export
left_join_preserve_order <- function(x, y, ...) {
  x$join_id_ <- seq_len(nrow(x))
  joined <- merge(x = x, y = y, all.x = TRUE, sort = FALSE, ...)

  cols <- unique(c(colnames(x), colnames(y)))
  joined <- joined[
    order(joined$join_id),
    cols[cols %in% colnames(joined) & cols != "join_id_"]
  ]
  rownames(joined) <- NULL
  return(joined)
}

#' Cleanup text form ANDES to play nice in Oracle
#'
#' @param df Dataframe
#' @param col_name String the name of the column to target in the dataframe
#' @param max_chars Numeric, maximum characters allowed in Oracle
#' @export
cleanup_text <- function(df, col_name = NULL, max_chars = NULL) {
  # get the col
  andes_text <- df[, which(names(df) == col_name)]
  # cleanup, remove line breaks from the text block
  andes_text <- gsub("\r?\n|\r", " ", andes_text)

  if (!is.null(max_chars) && nchar(andes_text) > max_chars) {
    # truncate the text to max_chars
    andes_text <- substr(andes_text, 1, max_chars)
    logger::log_warn(
      "The text in column {col_name} was truncated to {max_chars} characters."
    )
  }

  df[col_name] <- andes_text
  return(df)
}

#' Convert andes datetime string form DB to Oracle date format
#'
#' @param datetime_str The string representing the datetime from ANDES DB
#' @export
andes_str_to_oracle_date <- function(datetime_str) {
  # as posixlt
  # posixct_date <- unlist(lapply(date_str, parse_andes_datetime))
  posixct_date <- parse_andes_datetime(datetime_str)
  return(format(posixct_date, format = "%Y-%m-%d"))
}

#' Convert andes datetime string form DB to Oracle datetime format
#'
#' @param datetime_str The string representing the datetime from ANDES DB
#' @export
andes_str_to_oracle_datetime <- function(datetime_str) {
  # as posixlt
  # posixct_date <- unlist(lapply(date_str, parse_andes_datetime))
  posixct_date <- parse_andes_datetime(datetime_str)
  timezone_str <- "America/Toronto"
  return(format(
    posixct_date,
    format = "%Y-%m-%d %H:%M:%S",
    tz = timezone_str
  ))
}

#' Verify is the ANDES dattime string is in daylight savings time
#'
#' @param datetime_str The string representing the datetime from ANDES DB
#' @return A boolean representing if the time is in DST
#' @export
is_andes_time_str_dst <- function(datetime_str) {
  is_dst <- NA
  # ANDES DB times are in UTC
  posixct_date <- parse_andes_datetime(datetime_str)
  # to see if EST vs EDT, convert to America/Toronto and look at offset
  timezone_str <- "America/Toronto"

  utc_offset <- format(posixct_date, format = "%z", tz = timezone_str)
  if (is.na(utc_offset)) {
    logger::log_warn(
      "Could not determine if Daylight savings is active for date: {datetime_str}"
    )
    # stop("Could not determine if Daylight savings is active for date")
  } else if (utc_offset == "-0400") {
    is_dst <- TRUE
  } else if (utc_offset == "-0500") {
    is_dst <- FALSE
  } else {
    logger::log_warn(
      "Could not determine if Daylight savings is active for date: {datetime_str}"
    )
    # stop("Could not determine if Daylight savings is active for date")
  }
  return(is_dst)
}

#' Convert ANDES UTC time string and converts it to a POSIXct object
#'
#' @param andes_time_str The string representing the datetime from ANDES DB
#' @return POSIXct object representing the time in UTC
#' @export
parse_andes_datetime <- function(andes_time_str) {
  # if (is.na(andes_time_str)==TRUE) {
  #   return(NA)
  # }
  parsed_time <- as.POSIXct(
    andes_time_str,
    format = "%Y-%m-%d %H:%M:%S",
    tz = "UTC",
    optional = TRUE
  )
  # Convert ISO 8601 time to POSIXlt, ANDES DB time values are implicitly in UTC
  return(parsed_time)
}

#' Add a hard-coded column with a specific value to the dataframe
#'
#' @param df The original dataframe to modify with a new column
#' @param col_name The new column name
#' @param value The value to add to every row in this column.
#' To add null values in the column use NA and not NULL.
#' @return The original dataframe with the new column added
#' @export
add_hard_coded_value <- function(df, col_name = NULL, value = NULL) {
  if (is.null(col_name)) {
    logger::log_error("add_hard_coded_value was called with bad arguments")
    stop("Both col_name and value must be provided.")
  }
  if (is.null(value)) {
    logger::log_error(
      "A hard-coded NULL-value was added to column {col_name}"
    )
    # this is what sanitize_sql_value() actually ends up doing...
    logger::log_error(
      "DO NOT DO THIS! Please change to NA and switch to NULL when executing the statement"
    )
  } else {
    logger::log_info(
      "A hard-coded value of {value} was added to column {col_name}"
    )
  }
  # add a hard coded value to the dataframe
  df[col_name] <- value
  return(df)
}

#' Sanitize value to SQL statement
#'
#' It will wrap string with an extra set of single quotes.
#' It will escape every single quote by doubling it up
#' This usualy does nothing to the value itself except inject the NULL string for NA/null and empty strings
#' @param value The value to sanitize
#' @export
sanitize_sql_value <- function(value) {
  if (is.null(value) || is.na(value)) {
    return("NULL")
  } else if (is.character(value)) {
    if (nchar(value) == 0) {
      return("NULL")
    } else {
      # escape single quotes
      value <- gsub("'", "''", value)
      # wrap the whole in single single quotes
      value <- paste("'", value, "'", sep = "")
      return(value)
    }
  }
  return(value)
}

#' Convert coordinate to Oracle format
#'
#'        For example, the latitude of 47.155927
#'        is decomposed into:
#'        whole_degrees = 47
# '       whole_minutes = 9
#'        decimal_minues = 35562
#'        and yields: 4709.35562
#'
#'        The Oracle Coordinates (including longitude) are not negative.
#' @param coord Input coordinate
#' @return Formatted coordinate
#' @export
to_oracle_coord <- function(coord) {
  if (is.null(coord) || is.na(coord)) {
    return(NA)
  }
  degrees <- floor(abs(coord))
  minutes_decimal <- (abs(coord) - degrees) * 60

  # the return value is never negative, as per historical oracle data
  return(degrees * 100 + minutes_decimal)
}

#' Generate a SQL statement
#'
#' generate a SQL instert statement for the single dataframe row as a new row into table_name
#' The dataframe must have named columns that correspond to the columns of the table
#' The values must have the correct data types (there will be some SQL value sanitizing)
#' The statement will look like:`INSERT INTO \{table_name\} \{col_names_str\} VALUES \{col_values_str\}`
#' Where `\{col_names_str\}` is list of column names with parentheses: `(NO_RELEVE COD_NBPC ANNEE COD_TYP_STRATIF DATE_DEB_PROJET...)`
#' and `\{col_values_str\}` is list of column values with parentheses: `(36 4 2025 7 '2025-05-03'...)`
#'
#' @param df_row Dataframe row
#' @param table_name String
#' @export
generate_sql_insert_statement <- function(df_row, table_name) {
  col_names <- NULL
  # remove id col if present
  if ("id" %in% names(df_row)) {
    df_row$id <- NULL
  }

  for (col in colnames(df_row)) {
    col_names <- paste(col_names, col, sep = ", ")
  }
  # remove the leading comma and space
  col_names <- substr(col_names, 3, nchar(col_names))
  # add parenthesis
  col_names <- paste("(", col_names, ") ", sep = "")

  # run sanitize_sql_value() on every column of this row
  col_values_str <- lapply(df_row, sanitize_sql_value)
  # collapse all values into one string with columns separated by a comma
  col_values_str <- paste(unlist(col_values_str), collapse = ", ")
  # add parenthesis
  col_values_str <- paste("(", col_values_str, ") ", sep = "")
  # the list is now build, we can create the INSERT statement.
  statement <- paste(
    "INSERT INTO",
    table_name,
    col_names,
    "VALUES",
    col_values_str,
    ";",
    sep = " "
  )
  return(statement)
}

#'
#' Convert all dataframe cols named in the col_names to a numeric value
#' @param df the dataframe to modify
#' @param col_names a list of column names which will be converted to numeric
#' @export
cols_to_numeric <- function(df, col_names = NULL) {
  if (is.null(col_names)) {
    stop("Must supply a list of column names")
  }
  for (i in seq_len(length(col_names))) {
    if (!col_names[i] %in% names(df)) {
      logger::log_error(
        "Cannot convert type, {col_names[i]} is not a column name"
      )
      stop("Cannot convert type, not a column name")
    }
    logger::log_debug("Converting column {col_names[i]} to numeric")
    df[, names(df) == col_names[i]] <- as.numeric(df[,
      names(df) == col_names[i]
    ])
  }
  return(df)
}

#'
#' Checks if all dataframe cols named in the col_names contain NA
#' This is useful to validate if a dataframe can be written to a DB table (where some columns values cannot be null)
#' @param df The dataframe to modify
#' @param col_names A list of column names which will be converted to numeric
#' @returns A boolean representing if the dataframe is compliant.
#' @export
check_cols_contains_na <- function(df, col_names = NULL) {
  if (is.null(col_names)) {
    stop("Must supply a list of column names")
  }
  for (col_name in col_names) {
    if (!col_name %in% names(df)) {
      logger::log_error(
        "Cannot verify, {col_name} is not a column in the dataframe"
      )
      stop("Cannot verify, not a column name")
    }
    if (any(is.na(df[, col_name == names(df)]))) {
      logger::log_error("Found NA in column {col_name}")
      logger::log_error(
        "dataframe cannot be written as DB table. It contains NULL values in a column that should not."
      )
      return(FALSE)
    }
  }
  return(TRUE)
}

#' Make sure the columns listed in col_names are present in the dataframe
#'
#' @param df Dataframe, the dataframe to verify
#' @param col_names List of column names. This will verify if the names in the list are present.
#' @param coerce Logical, (FALSE by default) to see if the dataframe can be coerced into compliance
#' @returns A Logical representing if the dataframe is compliant.
#' @export
check_columns_present <- function(df, col_names = NULL, coerce = FALSE) {
  if (is.null(col_names)) {
    stop("Must supply a list of required columns to verify")
  }
  for (col_name in col_names) {
    if (!(col_name %in% names(df))) {
      if (!coerce) {
        logger::log_error(
          "Missing required column. The column {col_name} is not in dataframe"
        )
        return(FALSE)
      } else {
        logger::log_error(
          "Missing required column. Will add a NULL column"
        )
        stop("NOT IMPLEMENTED")
      }
    }
  }
  return(TRUE)
}

#' Make sure no other columns than the ones listed in col_names are present in the dataframe
#' @param df the dataframe to verify
#' @param col_names A list of column names. This will veridy if columns not in the list is present
#' @param coerce A boolean (false by default) to see if the dataframe can be coerced into compliance
#' @returns A boolean representing if the dataframe is compliant.
#' @export
check_other_columns <- function(df, col_names = NULL, coerce = FALSE) {
  if (is.null(col_names)) {
    stop("Must supply a list of required columns to verify")
  }
  for (col_name in names(df)) {
    if (!(col_name %in% col_names)) {
      if (!coerce) {
        logger::log_warn(
          "An unexpected column was found in the dataframe: {col_name}"
        )
        return(FALSE)
      } else {
        logger::log_error(
          "An unexpected column was found removed from the dataframe: {col_name}"
        )
        stop("NOT IMPLEMENTED, drop unneeded columns manually")
      }
    }
  }
  return(TRUE)
}

#' Make sure the columns listed in col_names have numeric (or NA) values
#' @param df the dataframe to verify
#' @param col_names A list of column names. This will veridy if columns not in the list is present
#' @param coerce A boolean (false by default) to see if the dataframe can be coerced into compliance
#' @returns A boolean representing if the dataframe is compliant.
#' @export
check_numeric_columns <- function(df, col_names = NULL, coerce = FALSE) {
  if (is.null(col_names)) {
    stop("Must supply a list of required columns to verify")
  }

  for (col_name in col_names) {
    col_class <- class(df[, names(df) == col_name])
    if (!(col_class %in% c("integer", "numeric"))) {
      if (!coerce) {
        logger::log_warn(
          "The dataframe contains a column that is with the wrong datatype. {col_name} needs to be a number."
        )
        return(FALSE)
      } else {
        logger::log_error(
          "An unexpected column was found removed from the dataframe: {col_name}"
        )
        # df[, names(df) == col_name] <- as.numeric(df[, names(df) == col_name])
        stop(
          "NOT IMPLEMENTED, use cols_to_numeric to prepare the dataframe"
        )
      }
    }
  }
  return(TRUE)
}

assert_col <- function(df, col_name) {
  if (!(col_name %in% names(df))) {
    logger::log_error("Missing column name from dataframe{col_name}")
    traceback()
    stop("Missing column")
  }
}

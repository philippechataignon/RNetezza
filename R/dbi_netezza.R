#' RNetezza
#'
#' Provides Access to Netezza Through the ODBC Interface An implementation of R's DBI interface using ODBC package as a back-end.
#'
#' @name RNetezza
#' @docType package
#' @import methods DBI RODBC
NULL
setOldClass("RODBC")

#' NetezzaDriver and methods.
#'
#' An Netezza driver implementing the R database (DBI) API.
#' This class should always be initialized with the \code{Netezza()} function.
#' It returns an object that allows you to connect to Netezza.
#'
#' @export
#' @keywords internal
setClass("NetezzaDriver", contains = "DBIDriver")

#' Generate an object of NetezzaDriver class
#'
#' This driver is for implementing the R database (DBI) API.
#' This class should always be initialized with the \code{Netezza()} function.
#' Netezza driver does nothing for Netezza connection. It just exists for S4 class compatibility with DBI package.
#'
#' @export
#' @examples
#' \dontrun{
#' driver <- RNetezza::Netezza()
#' # Connect to a Netezza data source
#' con <- dbConnect(driver, dsn="test")
#' # Always cleanup by disconnecting the database
#' #' dbDisconnect(con)
#' }
Netezza <- function() {new("NetezzaDriver")}


#' @rdname NetezzaDriver-class
#' @export
setMethod("dbUnloadDriver", "NetezzaDriver", function(drv, ...) {TRUE})

#' Connect/disconnect to a Netezza data source
#'
#' These methods are straight-forward implementations of the corresponding generic functions.
#'
#' @param drv an object of class NetezzaDriver
#' @param dsn Data source name you defined by Netezza data source administrator tool.
#' @param user User name to connect as.
#' @param password Password to be used if the DSN demands password authentication.
#' @param ... Other parameters passed on to methods
#' @export
#' @examples
#' \dontrun{
#' # Connect to a Netezza data source
#' con <- dbConnect(RNetezzaDBI::Netezza(), dsn="test")
#' # Always cleanup by disconnecting the database
#' #' dbDisconnect(con)
#' }
setMethod(
  "dbConnect",
  "NetezzaDriver",
  function(drv, dsn, ...){
    connection <- odbcConnect(dsn)
    new("NetezzaConnection", odbc=connection)
  }
)

#' @rdname NetezzaDriver-class
#' @export
setMethod("dbIsValid", "NetezzaDriver", function(dbObj) {TRUE})

#' Get NetezzaDriver metadata.
#'
#' Nothing to do for NetezzaDriver case
#'
#' @rdname NetezzaDriver-class
#' @export
setMethod("dbGetInfo", "NetezzaDriver", function(dbObj, ...) {NULL})


#' Class NetezzaConnection.
#'
#' \code{NetezzaConnection} objects are usually created by \code{\link[DBI]{dbConnect}}
#' @keywords internal
#' @export
setClass(
  "NetezzaConnection",
  contains="DBIConnection",
  slots=list(odbc="RODBC")
)

#' Execute a statement on a given database connection.
#'
#' To retrieve results a chunk at a time, use \code{dbSendQuery},
#' \code{dbFetch}, then \code{ClearResult}. Alternatively, if you want all the
#' results (and they'll fit in memory) use \code{dbGetQuery} which sends,
#' fetches and clears for you.
#'
#' @param conn An existing \code{\linkS4class{NetezzaConnection}}
#' @param statement  The SQL which you want to run
#' @param res An object of class \code{\linkS4class{NetezzaResult}}
#' @param n Number of rows to return. If less than zero returns all rows.
#' @param ... Other parameters passed on to methods
#' @export
#' @rdname odbc-query
setMethod("dbSendQuery", "NetezzaConnection", function(conn, statement, ...) {
  statement <- enc2utf8(statement)
  env <- new.env()
  assign("is_done", FALSE, envir=env)
  new("NetezzaResult", connection=conn, sql=statement, state=env)
})

#' Get DBMS metadata.
#'
#' @param dbObj An object inheriting from \code{\linkS4class{NetezzaConnection}}, \code{\linkS4class{NetezzaDriver}}, or a \code{\linkS4class{NetezzaResult}}
#' @param ... Other parameters passed on to methods
#' @export
setMethod("dbGetInfo", "NetezzaConnection", function(dbObj, ...) {
  info <- RODBC::odbcGetInfo(dbObj@odbc)
  list(dbname = unname(info["DBMS_Name"]),
       db.version = unname(info["DBMS_Ver"]),
       username = "",
       host = "",
       port = "",
       sourcename = unname(info["Data_Source_Name"]),
       servername = unname(info["Server_Name"]),
       drivername = unname(info["Driver_Name"]),
       odbc.version = unname(info["Netezza_Ver"]),
       driver.version = unname(info["Driver_Ver"]),
       odbcderiver.version = unname(info["Driver_Netezza_Ver"]))
})


#' List fields in specified table.
#'
#' @param conn An existing \code{\linkS4class{NetezzaConnection}}
#' @param name a length 1 character vector giving the name of a table.
#' @export
#' @examples
#' \dontrun{
#' library(DBI)
#' con <- dbConnect(RNetezzaDBI::Netezza(), dsn="test", user="sa", password="Password12!")
#' dbWriteTable(con, "iris", iris, overwrite=TRUE)
#' dbListFields(con, "iris")
#' dbDisconnect(con)
#' }
setMethod("dbListFields", c("NetezzaConnection", "character"), function(conn, name) {
  query <- paste0("SELECT * FROM ", name, " LIMIT 0")
  res <- sqlQuery(conn@odbc, query)
  names(res)
})

#' List available Netezza tables.
#'
#' @param conn An existing \code{\linkS4class{NetezzaConnection}}
#' @export
setMethod("dbListTables", "NetezzaConnection", function(conn){
  query <- "SELECT tablename as name FROM _v_table where objtype in ('TABLE', 'TEMP TABLE')
        union SELECT viewname as name FROM _v_view where objtype='VIEW'
        union SELECT synonym_name as name FROM _v_synonym where objtype='SYNONYM'
  "
  res <- sqlQuery(conn@odbc, query, believeNRows=F)
  as.character(res[[1]])
})

#' Write a local data frame or file to the database.
#'
#' @export
#' @rdname dbWriteTable
#' @param conn a \code{\linkS4class{NetezzaConnection}} object, produced by \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name. NetezzaConnection table names
#'   are \emph{not} case sensitive, e.g., table names \code{ABC} and \code{abc}
#'   are considered equal.
#' @param value a data.frame (or coercible to data.frame) object or a
#'   file name (character).  when \code{value} is a character, it is interpreted as a file name and its contents imported to Netezza.
#' @param overwrite logical. Should data be overwritten?
#' @param append logical. Should data be appended to an existing table?
#' @param ... additional arguments passed to the generic.
#' @export
#' @examples
#' \dontrun{
#' library(DBI)
#' con <- dbConnect(RNetezzaDBI::Netezza(), dsn="test", user="sa", password="Password12!")
#' dbWriteTable(con, "mtcars", mtcars, overwrite=TRUE)
#' dbReadTable(con, "mtcars")
#' dbDisconnect(con)
#' }
setMethod("dbWriteTable", c("NetezzaConnection", "character", "data.frame"), function(conn, name, value, overwrite=FALSE, append=FALSE, ...) {
  sqlSave(conn@odbc, dat=value, tablename=name, safer=!overwrite, append=append, ...)
  invisible(TRUE)
})

#' Does the table exist?
#'
#' @param conn An existing \code{\linkS4class{NetezzaConnection}}
#' @param name String, name of table. Match is case insensitive.
#' @return boolean value which indicated whether the table exists or not
#' @export
setMethod("dbExistsTable", c("NetezzaConnection", "character"), function(conn, name) {
  name %in% dbListTables(conn)
})

#' Remove a table from the database.
#'
#' Executes the SQL \code{DROP TABLE}.
#'
#' @param conn An existing \code{\linkS4class{NetezzaConnection}}
#' @param name character vector of length 1 giving name of table to remove
#' @export
setMethod("dbRemoveTable", c("NetezzaConnection", "character"), function(conn, name) {
  if(dbExistsTable(conn, name)){
    sqlDrop(conn@odbc, name)
  }
  invisible(TRUE)
})

#' Convenience functions for importing/exporting DBMS tables
#'
#' These functions mimic their R/S-Plus counterpart \code{get}, \code{assign},
#' \code{exists}, \code{remove}, and \code{objects}, except that they generate
#' code that gets remotely executed in a database engine.
#'
#' @return A data.frame in the case of \code{dbReadTable}; otherwise a logical
#' indicating whether the operation was successful.
#' @note Note that the data.frame returned by \code{dbReadTable} only has
#' primitive data, e.g., it does not coerce character data to factors.
#'
#' @param conn a \code{\linkS4class{NetezzaConnection}} object, produced by \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name.
#' @param row.names a character string specifying a table name.
#' @param check.names If \code{TRUE}, the default, column names will be converted to valid R identifiers.
#' @param select.cols  A SQL statement (in the form of a character vector of
#'    length 1) giving the columns to select. E.g. "*" selects all columns,
#'    "x,y,z" selects three columns named as listed.
#' @inheritParams DBI::rownamesToColumn
#' @export
#' @examples
#' \dontrun{
#' library(DBI)
#' con <- dbConnect(RNetezzaDBI::Netezza(), dsn="test", user="sa", password="Password12!")
#' dbWriteTable(con, "mtcars", mtcars, overwrite=TRUE)
#' dbReadTable(con, "mtcars")
#' dbGetQuery(con, "SELECT * FROM mtcars WHERE cyl = 8")
#'
#' # Supress row names
#' dbReadTable(con, "mtcars", row.names = FALSE)
#' dbGetQuery(con, "SELECT * FROM mtcars WHERE cyl = 8", row.names = FALSE)
#'
#' dbDisconnect(con)
#' }
setMethod("dbReadTable", c("NetezzaConnection", "character"), function(conn, name, row.names = NA, check.names = TRUE, select.cols = "*") {
  #out <- dbGetQuery(conn, paste("SELECT", select.cols, "FROM", name), row.names = row.names)
  out <- dbGetQuery(conn, paste("SELECT", select.cols, "FROM", name), row.names = row.names)
  if (check.names) {
    names(out) <- make.names(names(out), unique = TRUE)
  }
  out
})

#' Close a current session.
#'
#' @rdname dbDisconnect
#' @param conn a \code{\linkS4class{NetezzaConnection}} object, produced by \code{\link[DBI]{dbConnect}}
#' @examples
#' \dontrun{
#' library(DBI)
#' con <- dbConnect(RNetezzaDBI::Netezza(), dsn="test", user="sa", password="Password12!")
#' dbDisconnect(con)
#' }
#' @export
setMethod("dbDisconnect", "NetezzaConnection", function(conn) {
  if (RNetezza:::odbcValidChannel(conn@odbc)){
    odbcClose(conn@odbc)
  } else{
    TRUE
  }
})


#' Class NetezzaResult.
#'
#' Netezza's query results class. This classes encapsulates the result of an SQL statement (either select or not). The main generator is dbSendQuery.
#' @keywords internal
#' @export
setClass(
  "NetezzaResult",
  contains = "DBIResult",
  slots= list(
    connection="NetezzaConnection",
    sql="character",
    state="environment"
  )
)

is_done <- function(x) {
  x@state$is_done
}
`is_done<-` <- function(x, value) {
  x@state$is_done <- value
  x
}

#' @inheritParams DBI::rownamesToColumn
#' @export
#' @rdname odbc-query
setMethod("dbFetch", "NetezzaResult", function(res, n = -1, ...) {
  result <- sqlQuery(res@connection@odbc, res@sql, max=ifelse(n==-1, 0, n), believeNRows=F)
  is_done(res) <- TRUE
  result
})

#' @rdname odbc-query
#' @export
setMethod("dbHasCompleted", "NetezzaResult", function(res, ...) {
  is_done(res)
})

#' @rdname odbc-query
#' @export
setMethod("dbClearResult", "NetezzaResult", function(res, ...) {
  name <- deparse(substitute(res))
  is_done(res) <- FALSE
  TRUE
})


#' Database interface meta-data.
#'
#' See documentation of generics for more details.
#'
#' @param dbObj An object inheriting from \code{\linkS4class{NetezzaConnection}}, \code{\linkS4class{NetezzaDriver}}, or a \code{\linkS4class{NetezzaResult}}
#' @param res An object of class \code{\linkS4class{NetezzaResult}}
#' @param ... Ignored. Needed for compatibility with generic
#' @examples
#' \dontrun{
#' library(DBI)
#' data(USArrests)
#' con <- dbConnect(RNetezzaDBI::Netezza(), dsn="test", user="sa", password="Password12!")
#' dbWriteTable(con, "t1", USArrests, overwrite=TRUE)
#' dbWriteTable(con, "t2", USArrests, overwrite=TRUE)
#'
#' dbListTables(con)
#'
#' rs <- dbSendQuery(con, "select * from t1 where UrbanPop >= 80")
#' dbGetStatement(rs)
#' dbHasCompleted(rs)
#'
#' info <- dbGetInfo(rs)
#' names(info)
#' info$fields
#'
#' dbFetch(rs, n=2)
#' dbHasCompleted(rs)
#' info <- dbGetInfo(rs)
#' info$fields
#' dbClearResult(rs)
#'
#' # DBIConnection info
#' names(dbGetInfo(con))
#'
#' dbDisconnect(con)
#' }
#' @name odbc-meta
NULL

#' @rdname odbc-meta
#' @export
setMethod("dbGetRowCount", "NetezzaResult", function(res, ...) {
  df <- sqlQuery(res@connection@odbc, res@sql, believeNRows=F)
  nrow(df)
})

#' @rdname odbc-meta
#' @export
setMethod("dbGetStatement", "NetezzaResult", function(res, ...) {
  res@sql
})

#' @rdname odbc-meta
#' @export
setMethod("dbGetInfo", "NetezzaResult", function(dbObj, ...) {
  dbGetInfo(dbObj@connection)
})


#' @rdname odbc-meta
#' @export
setMethod("dbColumnInfo", "NetezzaResult", function(res, ...) {
  df <- sqlQuery(res@connection@odbc, res@sql, max=1)
  data_type <- sapply(df, class)
  data.frame(
    name=colnames(df),
    data.type=data_type,
    field.type=-1, #Can implement it(Data type in DBMS) through RNetezza
    len=-1,
    precision=-1,
    scale=-1,
    nullOK=sapply(df, function(x){any(is.null(x))}) #adhoc...
  )
})

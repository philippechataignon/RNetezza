#  RNetezza

RNetezza is an DBI-compliant interface to the Netezza database. 

#Installation

Install via github:

```R
install.packages('devtools')
devtools::install_github('philippechataignon/RNetezza')
```
## Basic usage

```R
library(DBI)
con <- dbConnect(RNetezza::Netezza(), dsn='NZN')

dbRemoveTable(con, 'mtcars')
dbWriteTable(con, "mtcars", mtcars)

dbListTables(con)
dbListFields(con, "mtcars")
dbReadTable(con, "mtcars")

# get dataframe
df <- dbGetQuery(con, 'SELECT * FROM "mtcars" WHERE "cyl" = 4')

# You can fetch all results:
res <- dbSendQuery(con, 'SELECT * FROM "mtcars" WHERE "cyl" = 4')
dbFetch(res)
dbClearResult(res)

# Or a chunk at a time
res <- dbSendQuery(con, 'SELECT * FROM "mtcars" WHERE "cyl" = 4')
# Error : no partial fetch
chunk <- dbFetch(res, n = 5)

# Clear the result
dbClearResult(res)

# Disconnect from the database
dbDisconnect(con)
```

#Credits

* [dplyr](https://github.com/hadley/dplyr)
* [RODBCDBI](https://github.com/teramonagi/RODBCDBI)

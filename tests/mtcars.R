library(DBI)
con <- dbConnect(RNetezza::Netezza(), dsn='NZN')

dbRemoveTable(con, 'mtcars')
dbWriteTable(con, "mtcars", mtcars)

dbListTables(con)
dbListFields(con, "mtcars")
dbReadTable(con, "mtcars")

# get df
df <- dbGetQuery(con, 'SELECT * FROM "mtcars" WHERE "cyl" = 4')

# You can fetch all results:
res <- dbSendQuery(con, 'SELECT * FROM "mtcars" WHERE "cyl" = 4')
dbFetch(res)
dbClearResult(res)

# Or a chunk at a time
res <- dbSendQuery(con, 'SELECT * FROM "mtcars" WHERE "cyl" = 4')
# Error
chunk <- dbFetch(res, n = 5)

# Clear the result
dbClearResult(res)

# Disconnect from the database
dbDisconnect(con)


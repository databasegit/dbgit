# Changelog

## [0.3.0.8b29a05] - 2019-08-24
### Added
 - Inserts and updates show in generated sql scripts with BLOB data
 - Improved output error messages
 
 ### fixes
 -- Fixed some restore and converting bugs
 
## [0.3.0.4b85c2d] - 2019-07-29
### Added
 - You can backup db object before change manually or automatically now
 - You can restore db into different db now (from Oracle to Postgresql or from Postgresql to Oracle)

## [0.3.0.427d28a] - 2019-07-29
### Added
- Different schemes for different database types in default .dbignore file
- Added dbgit link -d option, it creates default dblink that will be added to repository
- .dbignore entries are case insensitive if they do not quoted now
- Table columns restore with the same order as they were added to the source database

### fixes
- Fixed bug with Postgresql view restore
- Fixed bug with table restore when one table has foreign key to table than doesn't restore yet
- Fixed bug with boolean types in Postgresql restore adapter
- Fixed bug with primary keys in Postgresql
- Fixed some bugs with restore to Oracle database

## [0.2.2.1cf9dfd] - 2019-07-17
### Added
- All output messages store in yaml file now
- You can switch between localization files 
- Added `checkout -u`, `checkout --no-db`, `dump -u` options
- Added boolean type support for databases work with it
- No need to write `dbgit checkout -b branch remotes/origin/branch` to download branch from remote repository. It's enough to call `dbgit checkout -b branch`

### Changed
- `.dbgitignore` placed in `.dbgit` directory, and the file will be added to repository when commit

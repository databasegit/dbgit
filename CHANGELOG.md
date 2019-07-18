# Changelog

## [0.2.2] - 2019-07-17
### Added
- All output messages store in yaml file now
- You can switch between localization files 
- Added `checkout -u`, `checkout --no-db`, `dump -u` options
- Added boolean type support for databases work with it
- No need to write `dbgit checkout -b branch remotes/origin/branch` to download branch from remote repository. It's enough to call `dbgit checkout -b branch`

### Changed
- `.dbgitignore` placed in `.dbgit` directory, and the file will be added to repository when commit

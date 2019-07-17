# DBGIT

A tool to bind your database and git version control system

## Warnings/Caveats
- dbgit requires at least a Java 8 JDK
- You need Maven to build dbgit

## How to build

You need to have `Oracle JDBC driver` in your local Maven repository to build `dbgit`. For this:

1) download `ojdbc8-18.3.0.0.jar` by the next link (Oracle account required):

`https://www.oracle.com/content/secure/maven/content/com/oracle/jdbc/ojdbc8/18.3.0.0/ojdbc8-18.3.0.0.jar`

To create distribution files run from project root directory:

`$ mvn package appassembler:assemble`

After that you will get `target/dbgit` folder that contains the tool ready to install


2) run the next command:

`mvn install:install-file -DgroupId=com.oracle.jdbc -DartifactId=ojdbc8 -Dversion=18.3.0.0 -Dpackaging=jar -Dfile=<Path to jar_file>/ojdbc8-18.3.0.0.jar -DgeneratePom=true`

## Installation
- You can install `dbgit` in Windows with `dbgit-install-windows.bat` file as follows:

`dbgit-install-windows.bat`

- You can install `dbgit` in Linux with `dbgit-install-linux.sh` file as follows:

`sh dbgit-install-linux.sh`

This command will check if you have `JRE` and `Git` on your computer. If you don't, it will download and install them. 
After that the installer will check your `PATH` system variable. 
If the installer will find `dbgit` in `PATH` it will copy files to the path it found. 
Otherwise, the installer will set current directory to the `PATH` system variable, so current directory will be used as work folder for `dbgit`

`dbgit-install-windows.bat <path>`

After checking and installing (if needed) `JRE` and `Git` the installer will copy `dbgit` files to the folder you set.


## Quick start
First thing you need to do is to create git repository and bind `dbgit` with database. To create repository you can use one of next options:

`dbgit clone https://login:password@example.com/repo.git`

It does the same as `git clone`, it clones remote repository to your computer. Or you can run

`dbgit init`

`dbgit remote add origin https://login:password@example.com/repo.git`

These commands will create empty local repository and after that will bind it with remote repository. 

To bind your database with `dbgit` you need to run `dbgit link` with command like this:

`dbgit link jdbc:oracle:thin:@192.168.1.1:1521:SCHEME user=username password=pass`

`dbgit` is ready to use!

Make sure your Oracle user has grants to next views:

- DBA_ROLE_PRIVS
- DBA_OBJECTS
- DBA_SEQUENCES
- DBA_TABLES
- DBA_TAB_COLS
- DBA_USERS
- dba_segments

## Commands
Many of `dbgit` commands do the same work as the git commands with same names, but options are sometime specific to dbgit.
More support of git options will be implemented in the future versions.
You can run any of commands with `-h` option to get details.

##### dbgit-specific

- __link__

It binds `dbgit` with a database. Example:

`dbgit link jdbc:oracle:thin:@192.168.1.1:1521:SCHEME user=username password=pass`

This command creates `.dbignore` file that makes `dbgit` ignore all db objects except of user's scheme by default. You can reconfig `.dbignore` at any time, see Features for details

- __synonym__

will create synonym for database schemes, so you can use simple names if your db scheme has long or hard to writing name. Example:

  `dbgit synonym SYNONYM_NAME ACTUAL_SCHEME_NAME`

- __restore__

Restores db from the dbgit repository. This command doesn't change database by default, it creates sql file in `<repo_folder>/.dbgit/scripts`. Add `-r` option to make command change database. Option `-s` lets you save sql script to a specific without of execution. Examples:

`dbgit restore`

`dbgit restore -r`

`dbgit restore -s c:\temp\script.sql`

- __dump__

Dumps db objects into the dbgit repository. Runs without parameters

`dbgit dump`

- __valid__

Checks if dbgit data files are valid. Runs without parameters

`dbgit valid`

##### similar to git

- __clone__

This command will clone remote repository to your computer. Example:

`dbgit clone https://login:password@example.com/repo.git`

- __init__

It will create empty local repository, examples:

`dbgit init`

`dbgit init <c:\temp\repository>`

- __remote__

   with `add` parameter will add remote repository to your git config
   
   with `remove` parameter will remove remote repository from the git config
   
   with no parameters will show list of remote repositories
   
   `dbgit remote add origin https://git_user:git_password@gitrepository.org/repo.git`
   
   `dbgit remote remove stage`
   
   `dbgit remote`

- __status__

Runs without parameters, shows current status of database objects and local files of repository. It's analog of `git status`

`dbgit status`

- __add__

Adds db objects into the dbgit index. You can use mask to add many of db objects by one command. Example:

`dbgit add SCHEME/TEST_TABLE*`

`dbgit add SCHEME/TEST_VIEW.vw`

- __rm__

 Removes objects from the dbgit index. You can use mask to remove many of db objects by one command. If you will run it with `-db` option you will drop db object from database too. Example:
 
 `dbgit rm SCHEME/TABLE*`
 
- __checkout__

Switch branches or restore working tree files. Example:

`dbgit checkout master`

- __commit__

Makes git commit. Option `-m` lets you to add commit message. Option `-a` dumps and adds changes to index. Examples:

`dbgit commit -m Message`
`dbgit commit -a -m Message`
`dbgit commit file_name -m Message`

- __merge__

Join two or more development histories together

`dbgit merge dev`

- __pull__

Fetch from and integrate with another repository or a local branch

`dbgit pull`

- __fetch__

Download objects and refs from another repository

`dbgit fetch`

- __push__

Update remote refs along with associated objects. Runs without parameters

`dbgit push`

- __reset__

Reset current HEAD to the specified state

`dbgit reset -hard`

- __config__

Lets you configure `dbgit`. Example:

`dbgit config MAX_ROW_COUNT_FETCH=10000`

You can configure follow options:

`LIMIT_FETCH` - if true `dbgit` will save table data when table has less then specific number of rows

`MAX_ROW_COUNT_FETCH` - specifies max number of rows for table to save table data, if `LIMIT_FETCH` is true

`LOG_ROTATE` - number of days of log rotation

`SCRIPT_ROTATE` - number of days of sql files rotation

## Features

- You can run any command with `-v` option, it will show you full log of command execution then.

- `dbgit` can backup your db objects before it will change them. You can config backup settings, see __config__ command description for details.

- You can create and config file `.dbignore` to ignore some of database objects

`.dbignore` must be placed in repository root directory

Lets you exclude any of db objects from the work process. If name of db object will be mathed of regular expression you will write in this file it will be missed by the `dbgit`. Also, if you will write `!` as a first character of row, db object will be processed even if the one was excluded by previous expressions. `.dbgitignore` creates automatically when you run `dbgit link` command. By default `.dbignore` lets you work with your db user scheme only, and it ignores all table data. You can reconfigure it at any time.

## Usage

Let's guess, we have an Oracle database named `EXAMPLE` on server `192.168.1.2`, and we want to bind the scheme `TEST` with git repository placed on server `https://gitrep.com/repo.git`. For this, you need to execute next steps:

You need to move to directory you want to see as local repository. Then execute next commands:

`dbgit init`

`dbgit remote add origin https://<git_user_name>:<git_password>@gitrep.com/repo.git`

`dbgit link jdbc:oracle:thin:@192.168.1.2:1521:EXAMPLE user=TEST password=<db_user_password>`

By default `dbgit` will work with `TEST` schema only, and it will not work with tabledata, just description of db objects. Let's guess, you want to save to git repository all metadata of `TEST` schema, but also you want save data from table `CONFIG_TABLE` and package `MAIN_LOGIC` from schema `MANAGE`. For this you need to edit `.dbignore` file. Add next strings to the bottom of the file:

`!TEST/CONFIG_TABLE.csv`

`!MANAGE/MAIN_LOGIC.pkg` 

Make sure db user `TEST` has grants `SELECT_CATALOG_ROLE` - it needs to work with other schemes.

Then execute next command:

`dbgit add "*"`

It will save to local git repository all db objects. If you realize you added to local git repository db object you don't need (let's guess, it's `TEMPORARY_TABLE` table), you can remove it via next command:

`dbgit rm TEST/TEMPORARY_TABLE.tbl`

After that you can send your data to remote git repository with next commands:

`dbgit commit -m "init commit"`

`dbgit push`

Let's guess, another developer wants to work with your repository. For this he needs to execute:

`dbgit clone https://<git_user_name>:<git_password>@gitrep.com/repo.git`

`dbgit link jdbc:oracle:thin:@192.168.1.2:1521:EXAMPLE user=TEST password=<db_user_password>`

If he doesn't have db objects in database:

`dbgit restore -r`

After that, he can create the new branch to work:

`dbgit checkout -b dev`

After making changes in database:

`dbgit add "*"`

`dbgit commit -m "changes"`

`dbgit checkout master`

`dbgit merge dev`

`dbgit push`



## About Git

More information about Git, its repository format, and the canonical
C based implementation can be obtained from the
[Git website](http://git-scm.com/).
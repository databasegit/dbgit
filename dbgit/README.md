# DBGIT

A tool to let bind your database and git version control system

## Warnings/Caveats
- dbgit requires at least a Java 8 JDK
- You need Maven to build dbgit

## How to build
- To create distribution files run from project root directory:

`$ mvn package appassembler:assemble`

After that you will get `target/dbgit` folder that contains the tool ready to install

## Installation
- You can install `dbgit` in Windows with `dbgit-install-windows.bat` file. It has following options:

`dbgit-install-windows.bat`

This command will check if you have `JRE` and `Git` on your computer. If you don't it will download and install them. After that the installer will check your `PATH` system variable. If the installer will find `dbgit` in `PATH` it will copy files to the path it found. Otherwise, the installer will set current directory to the `PATH` system variable, so current directory will be used as work folder for `dbgit`

`dbgit-install-windows.bat <path>`

After checking and installing (if needed) `JRE` and `Git` the installer will copu `dbgit` files to the folder you set.

- Linux installer in progress...

## Quick start
First things you need to do to start work with `dbgit` are creating git repository and binding `dbgit` with database. To create repository you can use one of next options:

`dbgit clone https://login:password@example.com/repo.git`

It does the same as `git clone`, it clones remote repository to your computer. Or you can run

`dbgit init`

`dbgit remote add origin https://login:password@example.com/repo.git`

These commands will create empty local repository and after that will bind it with remote repository. 

To bind your database with `dbgit` you need to run `dbgit link` command like this example

`dbgit link jdbc:oracle:thin:@192.168.1.1:1521:SCHEME user=username password=pass`

`dbgit` is ready to use!

## Commands
Many of commands do the same as git commands with same names. Also, some of commands can run with switches. You can run any of commands with `-h` swith to get details

- __clone__

This command will clone remote repository to your computer. Example:

`dbgit clone https://login:password@example.com/repo.git`

- __init__

It will create empty local repository, examples:

`dbgit init`

`dbgit init <c:\temp\repository>`
 

- __link__

It binds `dbgit` with a database. Example:

`dbgit link jdbc:oracle:thin:@192.168.1.1:1521:SCHEME user=username password=pass`

- __remote__

   with `add` parameter will add remote repository to your git config
   
   with `remove` parameter will remove remote repository from the git config
   
   with no parameters will show list of remote repositories

- __synonym__

will create synonym for database schemes, so you can use simple names if your db scheme has long or hard to writing name. Example:

  `dbgit synonym SYNONYM_NAME ACTUAL_SCHEME_NAME`

- __status__

Runs without parameters, shows current status of database objects and local files of repository. It's analog of `git status`

- __add__

Adds db objects into the dbgit index. You can use mask to add many of db objects by one command. Example:

`dbgit add SCHEME/TEST_TABLE*`
`dbgit add SCHEME/TEST_VIEW.vw`

- __rm__

 Removes objects from the dbgit index. You can use mask to remove many of db objects by one command. If you will run it with `-db` swith you will drop db object from database too. Example:
 
 `dbgit rm SCHEME/TABLE*`
 
- __restore__

Restores db from the dbgit repository. Switch `-s` lets you save sql script to a file of restore without of execution. Examples:

`dbgit restore`

`dbgit restore -s c:\temp\script.sql`

- __dump__

Dumps db objects into the dbgit repository. Runs without parameters

- __valid__

Checks if dbgit data files are valid. Runs without parameters

- __checkout__

Switch branches or restore working tree files. Example:

`dbgit checkout master`

- __commit__

Makes git commit. Switch `-m` lets you to add commit message. Switch `-a` dumps and adds changes to index. Examples:

`dbgit commit -m Message`
`dbgit commit -a -m Message`
`dbgit commit file_name -m Message`

- __merge__

Join two or more development histories together

- __pull__

Fetch from and integrate with another repository or a local branch

- __push__

Update remote refs along with associated objects. Runs without parameters

## Features

- You can run any command with `-v` switch, it will show you full log of command execution then.

- You can create and config file `.dbignore` to ignore some of database objects

## About Git

More information about Git, its repository format, and the canonical
C based implementation can be obtained from the
[Git website](http://git-scm.com/).
You need to run dbgit-install-windows.bat to install dbgit on Windows. You can specify path as parameter, dbgit will be copied  to the path you specify then.
You can run installer without parameters, it will use the current dir to install.

Installer will check if you have JRE and Git on your computer. If you don't, it will download and install them.
You will need to restart your console after installing to use dbgit command in your OS.

Commands you can use you can see in help, that available if you run dbgit without parameters or with dbgit help parameter. 

First thing you need to do is to create git repository and bind dbgit with database. To create repository you can use one of next options:

    dbgit clone https://login:password@example.com/repo.git

It does the same as git clone, it clones remote repository to your computer. Or you can run

    dbgit init
    dbgit remote add origin https://login:password@example.com/repo.git

These commands will create empty local repository and after that will bind it with remote repository.

To bind your database with dbgit you need to run dbgit link with command like this:

    dbgit link jdbc:oracle:thin:@<SERVER_NAME>:<PORT>:<SID> user=<USER> password=<PASSWORD>

dbgit is ready to use!

Details and example of connection string you can see if you run command with -h switch
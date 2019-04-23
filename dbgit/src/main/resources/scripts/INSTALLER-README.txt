You need to run dbgit-install-windows.bat to install dbgit on Windows. You can specify path as parameter, dbgit will be copied  to the path you specify then.
You can run installer without parameters, it will use the current dir to install.

Installer will check if you have JRE and Git on your computer. If you don't, it will download and install them.

You will need to restart your console after installing to use dbgit command in your OS.

Commands you can use you can see in help, that available if you run dbgit without parameters or with dbgit help parameter. 

First step you need to do to work with dbgit is establishing connect with your database. You can do it via command 

    dbgit link <CONNECTION_STRING>

Details and example of connection string you can see if you run command with -h switch
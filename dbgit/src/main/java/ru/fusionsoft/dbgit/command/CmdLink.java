package ru.fusionsoft.dbgit.command;

import ru.fusionsoft.dbgit.core.DBConnection;

public class CmdLink implements IDBGitCommand {

	public void execute(String[] args) {
		// TODO Auto-generated method stub
		
		//создаем файл подключения
		DBConnection.createFileDBLink(/*params*/);
		
		//testConnect
		DBConnection.getInctance();
		
	}


}

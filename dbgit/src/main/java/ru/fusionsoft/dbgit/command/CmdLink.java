package ru.fusionsoft.dbgit.command;


import java.util.Arrays;
import java.util.Properties;

import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;


public class CmdLink implements IDBGitCommand {

	public void execute(String[] args) throws ExceptionDBGit {

		if(args == null || args.length == 0) {
			throw new ExceptionDBGit("Url database is empty");			
		}
		
		String url = args[0];
		Properties props = CreateProperties(Arrays.copyOfRange(args, 1, args.length));
		
		DBConnection conn = DBConnection.getInctance(false);
		
		if(conn.testingConnection(url, props)) {
			DBConnection.createFileDBLink(url, props);					
		}			
	}
	
	public Properties CreateProperties(String[] args) {
		Properties props = new Properties();
		for(String prop: args){
			String[] tmp = prop.split("=");
			props.put(tmp[0], tmp[1]);
		}
		return props;
	}
	

}

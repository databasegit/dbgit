package ru.fusionsoft.dbgit.command;


import java.util.Properties;

import ru.fusionsoft.dbgit.core.DBConnection;


public class CmdLink implements IDBGitCommand {

	public void execute(String[] args) {
		// TODO Auto-generated method stub
		if(args==null) {
			System.out.println("Url database is empty");
			
		}else {
			DBConnection conn = DBConnection.getInctance();
			String[] properties = CreateStringProperties(args);
			Properties props = CreateProperties(properties);
			
			if(conn.testingConnection(args[0], props)) {
				DBConnection.createFileDBLink(args[0], properties);
			}
			else {
				System.out.println("Connection no established!!!");
			}
		}
	}
	
	public Properties CreateProperties(String[] args) {
		Properties props = new Properties();
		for(String prop: args){
			props.put(prop.split("=")[0], prop.split("=")[1]);
		}
		return props;
	}
	public String[] CreateStringProperties(String[] args) {
		String[] props = new String[args.length-1];
		for(int i=0;i<props.length;i++) {
			props[i]=args[i+1];
		}
		return props;
	}

}

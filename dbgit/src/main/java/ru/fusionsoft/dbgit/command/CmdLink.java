package ru.fusionsoft.dbgit.command;

import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.core.DBGitPath;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CmdLink implements IDBGitCommand {

	public void execute(String[] args) {
		// TODO Auto-generated method stub
		if(args==null || args.length < 3) {
			System.out.println("Args length = " + args.length);
			for(String arg: args) {
				System.out.println(arg);
			}
		}else {
			Connection conn = null;
			String[] properties = CreateStringProperties(args);
			Properties props = CreateProperties(properties);
			if(testingConnection(conn, args[0], props)) {
				createFile(args[0], properties);
				
			}
			else {
				System.out.println("Connection no established!!!");
				return;
			}
		}
		
		
		/*
		
		System.out.println("excellent!!!");
		Properties props = CreateProperties(CreateStringProperties(args));
		try{
			Connection conn = DriverManager.getConnection(args[0], props);
			if(conn!=null) {
				String dbGitPath=DBGitPath.getFullPath(DBGitPath.DB_INFO_FILE);
				File file = new File(dbGitPath);
			    
				String filepath = file.getCanonicalPath();
				System.out.println("Link On!!!");
				System.out.println(filepath);
			}
		}catch(Exception e) {
			System.out.println(e.getMessage());
		}*/
		//создаем файл подключения
		//DBConnection.createFileDBLink(/*params*/);
		System.out.println("excellent!!!");
		
		//testConnect
		//DBConnection.getInctance();
		
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
	public boolean testingConnection(Connection conn, String url, Properties props) {
		try {
			conn = DriverManager.getConnection(url, props);
			System.out.println("Connection established");
			conn.close();
			return true;
		}catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
	public void createFile(String url, String[] props) {
		try{
			FileWriter writer = new FileWriter(DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE));		
		    writer.write("url="+url+"\n");
		    for(String prop: props) {
		    	writer.write(prop+"\n");
		    }
		    writer.close();
		    System.out.println("File " + DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE) + " has been created.");
	    }catch(Exception e) {
	    	e.printStackTrace();
	    }
	}
}

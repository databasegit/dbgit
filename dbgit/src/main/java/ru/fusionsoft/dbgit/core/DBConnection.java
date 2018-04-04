package ru.fusionsoft.dbgit.core;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Class for real connection to Database. Load parameters connection from .dblink file
 * DBConnection is Singleton
 * 
 * @author mikle
 *
 */
public class DBConnection {
	private static DBConnection dbGitConnection = null;
	private Connection connect = null;
	
	private DBConnection() {
		//TODO 
		//load file link and connect to bd 
		connect =  null; 
	}
	
	public static DBConnection getInctance() {
		if (dbGitConnection == null) {
			dbGitConnection = new DBConnection();
		}
		return dbGitConnection;
	}
	
	public Connection getConnect() {
		return connect;
	}
	public boolean testingConnection(String url, Properties props) {
		try {
			connect = DriverManager.getConnection(url, props);
			System.out.println("Connection established");
			connect.close();
			connect = null;
			return true;
		}catch(Exception e) {
			e.printStackTrace();
			return false;
		}
		
	}
	
	public static void createFileDBLink(String url, String[] props) {
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

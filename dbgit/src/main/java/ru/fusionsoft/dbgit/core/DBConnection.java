package ru.fusionsoft.dbgit.core;

import java.sql.Connection;

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
	
	public static void createFileDBLink(/*args*/) {
		
	}
}

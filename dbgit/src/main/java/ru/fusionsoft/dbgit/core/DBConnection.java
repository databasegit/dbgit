package ru.fusionsoft.dbgit.core;

import java.sql.Connection;

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

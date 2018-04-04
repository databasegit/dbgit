package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.Properties;

import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.LoggerUtil;

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
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	
	private DBConnection(Boolean isConnect) throws ExceptionDBGit {
		try {
			Properties props = new Properties();
			String url = loadFileDBLink(props);
			
			if (url != null) {
				connect = DriverManager.getConnection(url, props);
			}
		} catch(Exception e) {
			logger.error("Error connection to database", e);
			throw new ExceptionDBGit(e);
		}		
		 
	}
	public static DBConnection getInctance()  throws ExceptionDBGit {
		return getInctance(true);
	}
	
	public static DBConnection getInctance(Boolean isConnect)  throws ExceptionDBGit {
		if (dbGitConnection == null) {
			dbGitConnection = new DBConnection(isConnect);
		}
		return dbGitConnection;
	}
	
	public Connection getConnect() throws ExceptionDBGit {
		return connect;
	}
	public boolean testingConnection(String url, Properties props) {
		try {
			Connection conTest = DriverManager.getConnection(url, props);
			System.out.println("Connection established");
			conTest.close();
			conTest = null;
			return true;
		} catch(Exception e) {
			logger.error("Test connection error!", e);
			return false;
		}	
	}
	
	public static void createFileDBLink(String url, Properties props) throws ExceptionDBGit {
		try{			
			FileWriter writer = new FileWriter(DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE));		
		    writer.write("url="+url+"\n");
		    Enumeration e = props.propertyNames();

		    while (e.hasMoreElements()) {
		      String key = (String) e.nextElement();
		      writer.write(key+"="+ props.getProperty(key)+"\n");		      
		    }		   
		    writer.close();
		    System.out.println("File " + DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE) + " has been created.");
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static String loadFileDBLink(Properties props) throws ExceptionDBGit {
		try{			
			
			String filename = DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE);	
						
			File file = new File(filename);
			
			DBGitPath.createDir(file.getParent());
			
			if (!file.exists()) {
				return null;
			}
			
			FileInputStream fis = new FileInputStream(file);
			props.load(fis);
			fis.close();
			
			String url = props.getProperty("url");
			props.remove("url");
			
			return url;			
	    } catch(Exception e) {
	    	System.out.println("Error load file " + DBGitPath.DB_LINK_FILE);
	    	throw new ExceptionDBGit(e);
	    }
	}
}

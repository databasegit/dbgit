package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.Properties;

import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
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
				props.put("characterEncoding", "UTF-8");
				connect = DriverManager.getConnection(url, props);
				
				connect.setAutoCommit(false);
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
			ConsoleWriter.printlnGreen("Connection established");
			conTest.close();
			conTest = null;
			return true;
		} catch(Exception e) {
			ConsoleWriter.printlnRed("Test connection error: "+e.getMessage());			
			return false;
		}	
	}
	
	public static void createFileDBLink(String url, Properties props) throws ExceptionDBGit {
		try{	
			File file = new File(DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE));				
			DBGitPath.createDir(file.getParent());
			DBGitPath.createDefaultDbignore(DBGitPath.getRootPath(), props.getProperty("user").toUpperCase());
			DBGitPath.createDefaultDbgitConfig(DBGitPath.getFullPath());
			DBGitPath.createLogDir();
			DBGitPath.createScriptsDir();
						
			FileWriter writer = new FileWriter(file.getAbsolutePath());		
		    writer.write("url="+url+"\n");
		    Enumeration e = props.propertyNames();

		    while (e.hasMoreElements()) {
		      String key = (String) e.nextElement();
		      writer.write(key+"="+ props.getProperty(key)+"\n");		      
		    }		   
		    writer.close();
		    ConsoleWriter.println("File " + DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE) + " has been created.");
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static String loadFileDBLink(Properties props) throws ExceptionDBGit {
		try{			
			
			String filename = DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE);	
						
			File file = new File(filename);						
			
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
	    	throw new ExceptionDBGit("Error load file " + DBGitPath.DB_LINK_FILE, e);
	    }
	}
}

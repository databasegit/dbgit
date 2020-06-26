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
	private DBGitLang lang = DBGitLang.getInstance();

	private DBConnection(Boolean isConnect) throws ExceptionDBGit {
		try {
			Properties props = new Properties();
			String url = loadFileDBLink(props);
			
			if (url != null && isConnect) {
				props.put("characterEncoding", "UTF-8");
				connect = DriverManager.getConnection(url, props);
				connect.setAutoCommit(false);
			}
		} catch(Exception e) {
			logger.error(lang.getValue("errors", "connectionError").toString(), e);
			throw new ExceptionDBGit(e);
		}		
		 
	}
	
	public void flushConnection() {
		dbGitConnection = null;
	}
	
	public static DBConnection getInstance()  throws ExceptionDBGit {
		return getInstance(true);
	}
	
	public static DBConnection getInstance(Boolean isConnect)  throws ExceptionDBGit {
		if (dbGitConnection == null) {
			dbGitConnection = new DBConnection(isConnect);
		}
		return dbGitConnection;
	}
	
	public Connection getConnect() throws ExceptionDBGit {
		return connect;
	}
	
	public boolean testingConnection() {
		try {
			Properties props = new Properties();
			String url = loadFileDBLink(props);
			
			Connection conTest = DriverManager.getConnection(url, props);
			ConsoleWriter.printlnGreen(lang.getValue("general", "link", "connectionEstablished"));
			conTest.close();
			conTest = null;
			return true;
		} catch(Exception e) {
			ConsoleWriter.printlnRed(lang.getValue("errors", "link", "cantConnect") + ": " + e.getMessage());
			return false;
		}	
	}
	
	public boolean testingConnection(String url, Properties props) {
		try {
			Connection conTest = DriverManager.getConnection(url, props);
			ConsoleWriter.printlnGreen(lang.getValue("general", "link", "connectionEstablished"));
			conTest.close();
			conTest = null;
			return true;
		} catch(Exception e) {
			ConsoleWriter.printlnRed(lang.getValue("errors", "link", "cantConnect") + ": " + e.getMessage());
			return false;
		}	
	}
	
	public static void createFileDBLink(String url, Properties props, boolean isDefault) throws ExceptionDBGit {
		try{	
			File file;
			
			if (isDefault)
				file = new File(DBGitPath.getFullPath(DBGitPath.DB_LINK_DEF_FILE));
			else
				file = new File(DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE));				
			
			DBGitPath.createDir(file.getParent());			
			
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
		    ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "link", "dblinkCreated").withParams(DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE)));
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static String loadFileDBLink(Properties props) throws ExceptionDBGit {
		try{									
			File file = new File(DBGitPath.getFullPath(DBGitPath.DB_LINK_FILE));						
			File fileDef = new File(DBGitPath.getFullPath(DBGitPath.DB_LINK_DEF_FILE));
			
			if (!file.exists() && !fileDef.exists()) {
				return null;
			}
			
			FileInputStream fis = new FileInputStream(file.exists() ? file : fileDef);
			props.load(fis);
			fis.close();
			
			String url = props.getProperty("url");
			props.remove("url");
			
			return url;			
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "fileLoadError").withParams(DBGitPath.DB_LINK_FILE), e);
	    }
	}
}

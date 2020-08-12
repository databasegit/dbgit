package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileWriter;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.stream.Stream;

import javax.print.attribute.standard.DateTimeAtCompleted;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.Convertor;

/**
 * Different path use in project
 * 
 * @author mikle
 *
 */
public class DBGitPath {
	public static final String DB_GIT_PATH = ".dbgit";
	public static final String DB_SYNONYMS = ".synonyms";
	public static final String DB_LINK_FILE = ".dblink";
	public static final String DB_LINK_DEF_FILE = ".def_dblink";
	public static final String DB_IGNORE_FILE = ".dbignore";
	public static final String OBJECTS_PATH = ".objects";
	public static final String INDEX_FILE = ".dbindex";
	public static final String LOG_PATH = "logs";
	public static final String SCRIPT_PATH = "scripts";
	public static final String DATA_FILE = ".data";
	public static final String LOG_FILE = ".log";
	public static final String SQL_FILE = ".sql";
	public static final String DBGIT_CONFIG = "dbgitconfig";
	
	public static String idSession;
	
	static {
		idSession = Convertor.getGUID();
	}
	
	//path utils
	
	public static void createLogDir() throws ExceptionDBGit {
		if (DBGitPath.isRepositoryExists()) {
			File file = new File(getFullPath());
			
			if (file.exists()) {
				File logDir = new File(getLogsPath());
				if (!logDir.exists())
					logDir.mkdirs();
			}
		}		
	}

	public static void createScriptsDir() throws ExceptionDBGit {
		if (DBGitPath.isRepositoryExists()) {
			File file = new File(getFullPath());
			
			if (file.exists()) {
				File scriptsDir = new File(getScriptsPath());
				if (!scriptsDir.exists())
					scriptsDir.mkdirs();
			}
		}		
	}
	
	public static String getFullPath(String path) throws ExceptionDBGit {
		if (path == null) return getFullPath();
		return getFullPath() + path + "/";
	}
	
	public static String getLogsPath() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		return dbGit.getRootDirectory()+"/" + DB_GIT_PATH + "/" + LOG_PATH + "/";
	}
	
	public static String getLogsUserPath() throws ExceptionDBGit {
		return System.getProperty("user.home") + "/dbgit/" + LOG_PATH + "/";
	}
	
	public static String getScriptsPath() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		return dbGit.getRootDirectory()+"/" + DB_GIT_PATH + "/" + SCRIPT_PATH + "/";
	}
	
	public static String getFullPath() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		return dbGit.getRootDirectory()+"/"+DB_GIT_PATH + "/";
	}
	
	public static String getRootPath(String path) throws ExceptionDBGit {
		if (path == null) return getRootPath();
		return getRootPath() + path + "/";
	}
	
	public static boolean isRepositoryExists() throws ExceptionDBGit {
		return DBGit.checkIfRepositoryExists();		
	}
	
	public static String getRootPath() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		return dbGit.getRootDirectory() + "/";
	}
	
	public static boolean createDir(String path) {
		File dir = new File(path);
		return dir.mkdirs();
	}
	
	public static void deleteOldLogs() throws Exception {
		int logRotate = 31;
		int scriptRotate = 31; 
		
		if (isRepositoryExists()) {
			logRotate = DBGitConfig.getInstance().getInteger("core", "LOG_ROTATE", DBGitConfig.getInstance().getIntegerGlobal("core", "LOG_ROTATE", 31));
			scriptRotate = DBGitConfig.getInstance().getInteger("core", "SCRIPT_ROTATE", DBGitConfig.getInstance().getIntegerGlobal("core", "SCRIPT_ROTATE", 31));
			
			File logDir = new File(getLogsPath());
			File scriptDir = new File(getScriptsPath());
			
			if (logDir.exists()) {
				for (File entry : logDir.listFiles()) {
					if (entry.getName().compareTo("log-" + getLogsLastDate(logRotate) + ".log") < 0) {
						entry.delete();
					}
				}
			}

			if (scriptDir.exists()) {
				for (File entry : scriptDir.listFiles()) {
					if (entry.getName().compareTo("script-" + getLogsLastDate(scriptRotate) + "000000.sql") < 0) {
						entry.delete();
					}
				}
			}

		}
				
		File logUserDir = new File(getLogsUserPath());

		if (logUserDir.exists()) {
			for (File entry : logUserDir.listFiles()) {
				if (entry.getName().compareTo("log-" + getLogsLastDate(logRotate) + ".log") < 0) {
					entry.delete();
				}
			}
		}

	}
	
	public static boolean createDefaultDbgitConfig(String path) throws ExceptionDBGit {
		try {
			File dbIgnoreFile = new File(path + "/" + DBGIT_CONFIG);
			DBGitPath.createDir(dbIgnoreFile.getParent());
			FileWriter writer = new FileWriter(dbIgnoreFile.getAbsolutePath());
			
			writer.write("[core]\n");
			writer.write("MAX_ROW_COUNT_FETCH = 10000\n");
			writer.write("LIMIT_FETCH = true\n");
			writer.write("LOG_ROTATE = 31\n");
			writer.write("LANG = ENG\n");
			writer.write("SCRIPT_ROTATE = 31\n");
			writer.write("TO_MAKE_BACKUP = false\n");
			writer.write("BACKUP_TO_SCHEME = false\n");
			writer.write("BACKUP_TABLEDATA = true\n");
			writer.write("PORTION_SIZE = 50000\n");
			writer.write("TRY_COUNT = 1000\n");
			writer.write("TRY_DELAY = 10\n");
			
			writer.close();
			
			return true;
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static boolean createDefaultDbignore(String path, String url, Properties props) throws ExceptionDBGit {
		try {
			File dbIgnoreFile = new File(path + "/" + DB_IGNORE_FILE);
			DBGitPath.createDir(dbIgnoreFile.getParent());
			FileWriter writer = new FileWriter(dbIgnoreFile.getAbsolutePath());
			
			writer.write("*\n");
			
			for (DBGitMetaType value : DBGitMetaType.values()) { 
				if (!value.getValue().equals(DBGitMetaType.DbGitTableData.getValue()) 
						&& !value.getValue().equals(DBGitMetaType.DBGitRole.getValue()) 
						&& !value.getValue().equals(DBGitMetaType.DBGitUser.getValue()))
					writer.write("!" + AdapterFactory.createAdapter(DriverManager.getConnection(url, props)).getDefaultScheme() + "/*." + value.getValue() + "\n");
			}

			writer.close();
			
			return false;
	    } catch(Exception e) {
	    	e.printStackTrace();
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static boolean isServiceFile(String file) {
		return file.equals(DB_LINK_FILE)
				|| file.equals(DB_IGNORE_FILE)
				|| file.equals(INDEX_FILE)
				|| file.endsWith(DATA_FILE)
				|| file.endsWith(LOG_FILE)
				|| file.endsWith(SQL_FILE);
	}
	
	public static String getTempDirectory() {
		String property = "java.io.tmpdir";

	    String tempDir = System.getProperty(property)+"/"+DB_GIT_PATH+"/"+idSession;
	    
	    createDir(tempDir);
	    
	    return tempDir;
	}
	
	public static void clearTempDir() {
		String tempDir = getTempDirectory();
		deleteFolder(new File(tempDir));
	}
	
	public static void deleteFolder(File folder) {
	    File[] files = folder.listFiles();
	    if(files!=null) { //some JVMs return null for empty dirs
	        for(File f: files) {
	            if(f.isDirectory()) {
	                deleteFolder(f);
	            } else {
	                f.delete();
	            }
	        }
	    }
	    folder.delete();
	}
	
	private static String getLogsLastDate(int logRotate) {
		Calendar logCalendar = new GregorianCalendar();
		logCalendar.add(Calendar.DATE, -1*logRotate);
		return new SimpleDateFormat("yyyyMMdd").format(logCalendar.getTime());
	}
}

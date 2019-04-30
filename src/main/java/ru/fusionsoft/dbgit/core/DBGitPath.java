package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileWriter;
import java.util.stream.Stream;

import ru.fusionsoft.dbgit.meta.DBGitMetaType;
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
	public static final String DB_IGNORE_FILE = ".dbignore";
	public static final String OBJECTS_PATH = ".objects";
	public static final String INDEX_FILE = ".dbindex";
	public static final String LOG_PATH = ".logs";
	public static final String DATA_FILE = ".data";
	public static final String DBGIT_CONFIG = "dbgitconfig";
	
	public static String idSession;
	
	static {
		idSession = Convertor.getGUID();
	}
	
	//path utils
	
	public static String getFullPath(String path) throws ExceptionDBGit {
		if (path == null) return getFullPath();
		return getFullPath() + path + "/";
	}
	
	public static String getFullPath() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		return dbGit.getRootDirectory()+"/"+DB_GIT_PATH + "/";
	}
	
	public static String getRootPath(String path) throws ExceptionDBGit {
		if (path == null) return getRootPath();
		return getRootPath() + path + "/";
	}
	
	public static String getRootPath() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		return dbGit.getRootDirectory() + "/";
	}
	
	public static boolean createDir(String path) {
		File dir = new File(path);
		return dir.mkdirs();
	}
	
	public static boolean createDefaultDbgitConfig(String path) throws ExceptionDBGit {
		try {
			File dbIgnoreFile = new File(path + "/" + DBGIT_CONFIG);
			DBGitPath.createDir(dbIgnoreFile.getParent());
			FileWriter writer = new FileWriter(dbIgnoreFile.getAbsolutePath());
			
			writer.write("[core]\n");
			writer.write("MAX_ROW_COUNT_FETCH = 10000");
			writer.write("LIMIT_FETCH = true");
			
			writer.close();
			
			return true;
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static boolean createDefaultDbignore(String path, String userName) throws ExceptionDBGit {
		try {
			File dbIgnoreFile = new File(path + "/" + DB_IGNORE_FILE);
			DBGitPath.createDir(dbIgnoreFile.getParent());
			FileWriter writer = new FileWriter(dbIgnoreFile.getAbsolutePath());
			
			writer.write("*\n");
			
			for (DBGitMetaType value : DBGitMetaType.values()) { 
				if (!value.getValue().equals("csv"))
					writer.write("!" + userName + "/*." + value.getValue() + "\n");
			}

			writer.close();
			
			return false;
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static boolean isServiceFile(String file) {
		return file.equals(DB_LINK_FILE) || file.equals(DB_IGNORE_FILE) || file.equals(INDEX_FILE) || file.endsWith(DATA_FILE);
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
	
}

package ru.fusionsoft.dbgit.core;

import java.io.File;

/**
 * Different path use in project
 * 
 * @author mikle
 *
 */
public class DBGitPath {
	public static final String DB_GIT_PATH = ".dbgit";
	public static final String DB_LINK_FILE = ".dblink";
	public static final String DB_IGNORE_FILE = ".dbignore";
	public static final String OBJECTS_PATH = ".objects";
	public static final String INDEX_FILE = ".dbindex";
	public static final String LOG_PATH = ".logs";
	
	//path utils
	
	public static String getFullPath(String path) throws ExceptionDBGit {
		if (path == null) return getFullPath();
		return getFullPath() + path + "/";
	}
	
	public static String getFullPath() throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInctance();
		return dbGit.getRootDirectory()+"/"+DB_GIT_PATH + "/";
	}
	
	public static boolean createDir(String path) {
		File dir = new File(path);
		return dir.mkdirs();
	}
	
	public static boolean isServiceFile(String file) {
		return file.equals(DB_LINK_FILE) || file.equals(DB_IGNORE_FILE) || file.equals(INDEX_FILE);
	}
	
	
}

package ru.fusionsoft.dbgit.core;

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
	public static final String DB_INFO_FILE = ".dbinfo";
	public static final String OBJECTS_PATH = ".objects";
	
	//path utils
	
	public static String getFullPath(String path) {
		return DB_GIT_PATH + "/" + path + "/";
	}
}

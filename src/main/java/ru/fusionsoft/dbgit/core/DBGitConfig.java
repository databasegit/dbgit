package ru.fusionsoft.dbgit.core;

import java.io.File;

import org.ini4j.Ini;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBGitConfig {
	
	private static DBGitConfig config = null;
	private Ini ini;

	private DBGitConfig() throws Exception {
		File file = new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG);
		if (file.exists())
			ini = new Ini(new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG));
	}
	
	public static DBGitConfig getInstance() throws Exception {
		if (config == null) 
			config = new DBGitConfig();

		return config; 
	}
	
	public String getString(String section, String option, String defaultValue) {
		try {
			String result = ini.get(section, option);
			return result == null ? defaultValue : result;
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public Boolean getBoolean(String section, String option, Boolean defaultValue) {
		try {
			String result = ini.get(section, option);
			
			return result == null ? defaultValue : Boolean.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public Integer getInteger(String section, String option, Integer defaultValue) {		
		try {
			String result = ini.get(section, option);
			return result == null ? defaultValue : Integer.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public Double getDouble(String section, String option, Double defaultValue) {
		try {
			String result = ini.get(section, option);
			return result == null ? defaultValue : Double.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public void setValue(String parameter, String value) throws ExceptionDBGit {
		try {
			if (!ini.get("core").containsKey(parameter))
				ConsoleWriter.println("There is no parameter " + parameter);
			else {			
				ini.get("core").put(parameter, value);
				ini.store(new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG));
			}
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}
	
}

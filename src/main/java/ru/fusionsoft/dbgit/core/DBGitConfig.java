package ru.fusionsoft.dbgit.core;

import org.ini4j.Ini;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.io.File;

public class DBGitConfig {
	
	private static DBGitConfig config = null;	
	private Ini ini = null;
	private Ini iniGlobal = null;

	private DBGitConfig() throws Exception {
		
		if (DBGit.checkIfRepositoryExists()) {
			File file = new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG);
			if (file.exists())
				ini = new Ini(file);
		}

		StringBuilder path = new StringBuilder(new File(
				DBGitConfig.class.getProtectionDomain().getCodeSource().getLocation().toURI()
		).getAbsolutePath());
		
		while (!new File(path + "/dbgitconfig").exists()) {
			path.append("/..");
		}
		
		if (new File(path + "/bin/dbgitconfig").exists())
			path.append("/bin/dbgitconfig");
		else
			path.append("/dbgitconfig");
		
		File fileGlobal = new File(path.toString());
		if (fileGlobal.exists())
			iniGlobal = new Ini(fileGlobal);
	}
	
	public static DBGitConfig getInstance() throws Exception {
		if (config == null) 
			config = new DBGitConfig();

		return config; 
	}
	
	public String getString(String section, String option, String defaultValue) {
		return getString(section, option, defaultValue, false);
	}
	
	public Boolean getBoolean(String section, String option, Boolean defaultValue) {
		return getBoolean(section, option, defaultValue, false);
	}
	
	public Integer getInteger(String section, String option, Integer defaultValue) {
		return getInteger(section, option, defaultValue, false);
	}
	
	public Double getDouble(String section, String option, Double defaultValue) {
		return getDouble(section, option, defaultValue, false);
	}
	
	public String getStringGlobal(String section, String option, String defaultValue) {
		return getString(section, option, defaultValue, true);
	}
	
	public Boolean getBooleanGlobal(String section, String option, Boolean defaultValue) {
		return getBoolean(section, option, defaultValue, true);
	}
	
	public Integer getIntegerGlobal(String section, String option, Integer defaultValue) {
		return getInteger(section, option, defaultValue, true);
	}
	
	public Double getDoubleGlobal(String section, String option, Double defaultValue) {
		return getDouble(section, option, defaultValue, true);
	}
	
	private String getString(String section, String option, String defaultValue, boolean global) {
		try {
			String result = global ? iniGlobal.get(section, option) : ini.get(section, option);
			return result == null ? defaultValue : result;
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	private Boolean getBoolean(String section, String option, Boolean defaultValue, boolean global) {
		try {
			String result = global ? iniGlobal.get(section, option) : ini.get(section, option);
			
			return result == null ? defaultValue : Boolean.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	private Integer getInteger(String section, String option, Integer defaultValue, boolean global) {		
		try {
			String result = global ? iniGlobal.get(section, option) : ini.get(section, option);
			return result == null ? defaultValue : Integer.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	private Double getDouble(String section, String option, Double defaultValue, boolean global) {
		try {
			String result = global ? iniGlobal.get(section, option) : ini.get(section, option);
			return result == null ? defaultValue : Double.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}
	
	public void setValue(String parameter, String value) throws ExceptionDBGit {
		setValue(parameter, value, false);
	}
	
	public void setValueGlobal(String parameter, String value) throws ExceptionDBGit {
		setValue(parameter, value, true);
	}
	
	public void setValue(String parameter, String value, boolean global) throws ExceptionDBGit {
		try {
			if (global) {
				if (!iniGlobal.get("core").containsKey(parameter))
					ConsoleWriter.println(DBGitLang.getInstance().getValue("errors", "config", "noParameter").withParams(parameter));
				else {			
					iniGlobal.get("core").put(parameter, value);
					iniGlobal.store(iniGlobal.getFile());		
				}
			} else {
				if (ini == null) 
					throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "gitRepNotFound"));
				
				if (!ini.get("core").containsKey(parameter))
					ConsoleWriter.println(DBGitLang.getInstance().getValue("errors", "config", "noParameter").withParams(parameter));
				else {			
					ini.get("core").put(parameter, value);
					ini.store(new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG));
				}
			}
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}
	
}

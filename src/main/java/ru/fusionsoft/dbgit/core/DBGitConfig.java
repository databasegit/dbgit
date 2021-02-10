package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.ini4j.Ini;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBGitConfig {
	private static int messageLevel = 1;
	private static DBGitConfig config = null;
	private Ini ini = null;
	private Ini iniGlobal = null;
	private Map<String, String> transientConfig = new HashMap<>();

	private DBGitConfig() throws Exception {

		if (DBGit.checkIfRepositoryExists()) {
			File file = new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG);
			if (file.exists())
				ini = new Ini(file);
		}

		String path = new File(DBGitConfig.class.getProtectionDomain().getCodeSource().getLocation()
				.toURI()).getAbsolutePath();

		//for debug:
		if (path.contains("classes")) path = path + "/../dbgit";

		while (!new File(path + "/dbgitconfig").exists()) {
			int i = path.lastIndexOf("/");
			if (i == -1) i = path.lastIndexOf("\\");

			path = path.substring(0, i);
		}

		if (new File(path + "/bin/dbgitconfig").exists())
			path = path + "/bin/dbgitconfig";
		else
			path = path + "/dbgitconfig";

		File fileGlobal = new File(path);
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
			String result = global ? iniGlobal.get(section, option) : getIni().get(section, option);
			return result == null ? defaultValue : result;
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private Ini getIni() throws ExceptionDBGit, IOException {
		if(ini == null){
			if (DBGit.checkIfRepositoryExists()) {
				File file = new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG);
				if (file.exists())
					ini = new Ini(file);
			}
		}
		return  ini;
	}

	private Boolean getBoolean(String section, String option, Boolean defaultValue, boolean global) {
		try {
			String result = global ? iniGlobal.get(section, option) : getIni().get(section, option);

			return result == null ? defaultValue : Boolean.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private Integer getInteger(String section, String option, Integer defaultValue, boolean global) {
		try {
			String result = global ? iniGlobal.get(section, option) : getIni().get(section, option);
			return result == null ? defaultValue : Integer.valueOf(result);
		} catch (Exception e) {
			return defaultValue;
		}
	}

	private Double getDouble(String section, String option, Double defaultValue, boolean global) {
		try {
			String result = global ? iniGlobal.get(section, option) : getIni().get(section, option);
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
					ConsoleWriter.detailsPrintln(
						DBGitLang.getInstance().getValue("errors", "config", "noParameter").withParams(parameter)
						, messageLevel
					);
				else {			
					iniGlobal.get("core").put(parameter, value);
					iniGlobal.store(iniGlobal.getFile());
				}
			} else {
				if (getIni() == null)
					throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "gitRepNotFound"));

				if (!ini.get("core").containsKey(parameter))
					ConsoleWriter.detailsPrintln(DBGitLang.getInstance().getValue("errors", "config", "noParameter").withParams(parameter), messageLevel);
				else {			
					getIni().get("core").put(parameter, value);
					getIni().store(new File(DBGitPath.getFullPath() + "/" + DBGitPath.DBGIT_CONFIG));
				}
			}
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
	}


	public static String TO_IGNORE_OWNER = "noowner";

	public void setValueTransient(String parameter, String value) throws ExceptionDBGit{
		transientConfig.put(parameter, value);
	}

	public String getValueTransient(String parameter, String defaultValue){
		return transientConfig.getOrDefault(parameter, defaultValue);
	}

	public void setToIgnoreOnwer(boolean value) throws ExceptionDBGit{
		transientConfig.put(TO_IGNORE_OWNER, value ? "true" : "false");
	}

	public boolean getToIgnoreOnwer(boolean defaultValue){
		return Boolean.valueOf(getValueTransient(TO_IGNORE_OWNER, defaultValue ? "true" : "false"));
	}
}

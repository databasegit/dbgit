package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class DBGitLang {
	private static DBGitLang lang = null;
	private Map<String, Object> mapValue;
	private String value;
	
	@SuppressWarnings("unchecked")
	private DBGitLang() {
		try {
			String path = new File(DBGitLang.class.getProtectionDomain().getCodeSource().getLocation()
				    .toURI()).getAbsolutePath();
			
			while (!new File(path + "/lang").exists()) 
				path += "/..";

			mapValue = (Map<String, Object>) 
					new Yaml().load(new FileInputStream(new File(path + "/lang/" + 
			DBGitConfig.getInstance().getString("core", "LANG", DBGitConfig.getInstance().getStringGlobal("core", "LANG", "no")) + ".yaml")));			
		} catch (Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}
	
	public static DBGitLang getInstance() {
		if (lang == null) lang = new DBGitLang();		
		return lang;
	}
	
	public DBGitLang getValue(String... args) {
		Object val = mapValue;
		value = "";		
		for (String arg : args) {
			@SuppressWarnings("unchecked")
			Map<String, Object> newVal = (Map<String, Object>) val;
	        val = newVal.get(arg);
	    }
		
		if (val != null)
			value = val.toString();
		
		return this;
	}
	
	public String withParams(String... args) {
		int i = 0;
		String newValue = value;
		for (String arg : args) {
			newValue = newValue.replace("{" + i + "}", arg);			
			i++;
		}
		
		return newValue;				
	}
	
	@Override
	public String toString() {
		return value;
	}
}

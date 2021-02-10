package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBGitLang {
	private static DBGitLang lang = null;
	private Map<String, Object> mapValue;
	private String value;
	
	@SuppressWarnings("unchecked")
	private DBGitLang() {
		try {			
			String path = new File(DBGitLang.class.getProtectionDomain().getCodeSource().getLocation()
				    .toURI()).getAbsolutePath();

			//for debug:
			if (path.contains("classes")) path = path + "/../dbgit";

			while (!new File(path + "/lang").exists()) {
				int i = path.lastIndexOf("/");
				if (i == -1) i = path.lastIndexOf("\\");
				
				path = path.substring(0, i);
			}
			
			mapValue = new Yaml().load(
				new FileInputStream(
					new File(
					path
					+ "/lang/"
					+ DBGitConfig.getInstance().getString("core", "LANG", DBGitConfig.getInstance().getStringGlobal("core", "LANG", "no")).toLowerCase()
					+ ".yaml")
				)
			);
		} catch (Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}
	
	public static DBGitLang getInstance() {
		if (lang == null) lang = new DBGitLang();		
		return lang;
	}
	
	public DBGitLang getValue(String... args) throws ExceptionDBGitRunTime {
		Object val = mapValue;
		value = "";		
		for (String arg : args) {
			@SuppressWarnings("unchecked")
			Map<String, Object> newVal = (Map<String, Object>) val;
	        val = newVal.get(arg);
	    }
		
		if (val != null){
			value = val.toString();
			if(value.equals("")) throw new ExceptionDBGitRunTime("Empty `lang` value for " + Arrays.toString(args));
		}
		else {
			throw new ExceptionDBGitRunTime("Cannot find `lang` value for " + Arrays.toString(args));
		}
		
		return this;
	}
	
	public String withParams(String... args) {
		int i = 0;
		String newValue = value;
		for (String arg : args) {
			newValue = newValue.replace("{" + i + "}", arg == null ? "" : arg);			
			i++;
		}
		
		return newValue;				
	}
	
	@Override
	public String toString() {
		return value;
	}
}

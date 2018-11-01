package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import ru.fusionsoft.dbgit.utils.Util;


public class SchemaSynonim {
	private static SchemaSynonim ss;
	
	private Map<String, String> mapSchema = new HashMap<>();
	private Map<String, String> mapSynonim = new HashMap<>();
	
	public static SchemaSynonim getInctance() {
		if (ss == null) {
			ss = new SchemaSynonim();
		}
		return ss;
	}
	
	public SchemaSynonim() {
		loadFile();
	}
	
	//@SuppressWarnings("unchecked")
	public void loadFile() {
		try{						
			String filename = DBGitPath.getFullPath(DBGitPath.DB_SYNONIMS);							
			File file = new File(filename);						
			
			if (!file.exists()) {
				return ;
			}
			
			FileInputStream fis = new FileInputStream(file);
			
	        Yaml yaml = new Yaml();
	        mapSchema = yaml.load(fis);
	        for (Map.Entry<String, String> entry : mapSchema.entrySet()) {
	        	mapSynonim.put(entry.getValue(), entry.getKey());
	        }
			
			fis.close();			
				
	    } catch(Exception e) {
	    	throw new ExceptionDBGitRunTime("Error load file " + DBGitPath.DB_SYNONIMS, e);
	    }
	}
	
	public void saveFile() throws ExceptionDBGit {
		try{				
			File file = new File(DBGitPath.getFullPath(DBGitPath.DB_SYNONIMS));				
			DBGitPath.createDir(file.getParent());
			
			FileOutputStream fis = new FileOutputStream(file);	
			
			Yaml yaml = new Yaml();
			String output = yaml.dump(mapSchema);
			fis.write(output.getBytes(Charset.forName("UTF-8")));
		    	
			fis.close();		    
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }		
	}
	
	
	
	public Map<String, String> getMapSchema() {
		return mapSchema;
	}

	public Map<String, String> getMapSynonim() {
		return mapSynonim;
	}

	public int getCountSynonim() {
		return mapSchema.size();
	} 
	
	public String getSynonim(String schema) {
		return mapSynonim.get(schema);
	}
	
	public String getSchema(String synonim) {
		return mapSchema.get(synonim);
	}
	
	public String getSynonimNvl(String schema) {
		return Util.nvl(mapSynonim.get(schema), schema);
	}
	
	public String getSchemaNvl(String synonim) {
		return Util.nvl(mapSchema.get(synonim), synonim);
	}
	
	public void addSchemaSynonim(String schema, String synonim) throws ExceptionDBGit {
		if (mapSchema.containsKey(synonim)) {
			throw new ExceptionDBGit("Synonim: "+synonim+" exists!");
		}
		if (mapSynonim.containsKey(schema)) {
			throw new ExceptionDBGit("Schema: "+schema+" exists!");
		}
		mapSchema.put(synonim, schema);
		mapSynonim.put(schema, synonim);
	}
	
	public void deleteBySynonim(String synonim) {
		String schema = mapSchema.get(synonim);
		mapSynonim.remove(schema);
		mapSchema.remove(synonim);
	}
	
	public void deleteBySchema(String schema) {
		String synonim = mapSchema.get(schema);
		mapSchema.remove(synonim);
		mapSynonim.remove(schema);
	}
	

}

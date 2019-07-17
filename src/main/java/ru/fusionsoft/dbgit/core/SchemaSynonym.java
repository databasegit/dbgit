package ru.fusionsoft.dbgit.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.utils.Util;


public class SchemaSynonym {
	private static SchemaSynonym ss;
	
	private Map<String, String> mapSchema = new HashMap<>();
	private Map<String, String> mapSynonym = new HashMap<>();
	DBGitLang lang = DBGitLang.getInstance();
	
	public static SchemaSynonym getInctance() throws ExceptionDBGit {
		if (ss == null) {
			ss = new SchemaSynonym();
		}
		return ss;
	}
	
	public SchemaSynonym() throws ExceptionDBGit {
		loadFile();
	}
	
	//@SuppressWarnings("unchecked")
	public void loadFile() throws ExceptionDBGit {
		try{						
			String filename = DBGitPath.getFullPath(DBGitPath.DB_SYNONYMS);							
			File file = new File(filename);						
			
			if (!file.exists()) {
				return ;
			}
			
			FileInputStream fis = new FileInputStream(file);
			
	        Yaml yaml = new Yaml();
	        mapSchema = yaml.load(fis);
	        for (Map.Entry<String, String> entry : mapSchema.entrySet()) {
	        	mapSynonym.put(entry.getValue(), entry.getKey());
	        }
			
			fis.close();			
				
	    } catch(Exception e) {
	    	throw new ExceptionDBGitRunTime(lang.getValue("errors", "fileLoadError").withParams(DBGitPath.DB_SYNONYMS), e);
	    } 
	}
	
	public void saveFile() throws ExceptionDBGit {
		try{				
			File file = new File(DBGitPath.getFullPath(DBGitPath.DB_SYNONYMS));				
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

	public Map<String, String> getMapSynonym() {
		return mapSynonym;
	}

	public int getCountSynonym() {
		return mapSchema.size();
	} 
	
	public String getSynonym(String schema) {
		return mapSynonym.get(schema);
	}
	
	public String getSchema(String synonym) {
		return mapSchema.get(synonym);
	}
	
	public String getSynonymNvl(String schema) {
		return Util.nvl(mapSynonym.get(schema), schema);
	}
	
	public String getSchemaNvl(String synonym) {
		return Util.nvl(mapSchema.get(synonym), synonym);
	}
	
	public void addSchemaSynonym(String schema, String synonym) throws ExceptionDBGit {
		if (mapSchema.containsKey(synonym)) {
			throw new ExceptionDBGit(lang.getValue("errors", "synonym", "synonymExists").withParams(synonym));
		}
		if (mapSynonym.containsKey(schema)) {
			throw new ExceptionDBGit(lang.getValue("errors", "synonym", "schemeExists").withParams(schema));
		}

		Map<String, DBSchema> schemes = AdapterFactory.createAdapter().getSchemes();
		
		if (!schemes.containsKey(schema))
			throw new ExceptionDBGit(lang.getValue("errors", "synonym", "schemeDoesntExist").withParams(schema));
		
		mapSchema.put(synonym, schema);
		mapSynonym.put(schema, synonym);
	}
	
	public void deleteBySynonym(String synonym) {
		String schema = mapSchema.get(synonym);
		mapSynonym.remove(schema);
		mapSchema.remove(synonym);
	}
	
	public void deleteBySchema(String schema) {
		String synonym = mapSchema.get(schema);
		mapSchema.remove(synonym);
		mapSynonym.remove(schema);
	}
	

}

package ru.fusionsoft.dbgit.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

/**
 * Class for with ignore database objects from .dbignore file
 * DBGitIgnore is Singleton
 * 
 * @author mikle
 *
 */
public class DBGitIgnore {
	private static DBGitIgnore ignore = null;
	
	private Map<String, MaskFilter> filters = new HashMap<>();
	
	private Map<String, MaskFilter> exclusions = new HashMap<>();
	
	DBGitIgnore() throws ExceptionDBGit {
		// load file DBIgnore
		loadFileDBIgnore();		
	}

	public DBGitIgnore(Map<String, MaskFilter> filters, Map<String, MaskFilter> exclusions) {
		this.filters = filters;
		this.exclusions = exclusions;
	}

	protected void loadFileDBIgnore() throws ExceptionDBGit {
		loadFileDBIgnore(false);
	}
	protected void loadFileDBIgnore(boolean withBackupSchemas) throws ExceptionDBGit {
		try{				
			File file = new File(DBGitPath.getRootPath(DBGitPath.DB_GIT_PATH+"/"+DBGitPath.DB_IGNORE_FILE));
			
			if (!file.exists()) return ;
			//boolean isBackupToScheme = DBGitConfig.getInstance().getBoolean("core", "BACKUP_TO_SCHEME", false);
			
			BufferedReader br = new BufferedReader(new FileReader(file));			
			for(String line; (line = br.readLine()) != null; ) {

				
				if (line.startsWith("!")) {
					MaskFilter mask = new MaskFilter(line.substring(1));
					exclusions.put(line.substring(1), mask);
					if(withBackupSchemas /*&&isBackupToScheme*/){
						String schemaPart = line.substring(1,line.indexOf("/")+1);
						String backupSchemeObjExclusion =  line.replace(schemaPart, DBBackupAdapter.PREFIX + schemaPart).substring(1);
						MaskFilter backupSchemeObjMask = new MaskFilter(backupSchemeObjExclusion);
						exclusions.put(backupSchemeObjExclusion, backupSchemeObjMask);
					}
				}
				else {
					MaskFilter mask = new MaskFilter(line);
					filters.put(line, mask);
				}
			}
			    
			br.close();		    
	    } catch(Exception e) {
	    	throw new ExceptionDBGit(e);
	    }
	}
	
	public static DBGitIgnore getInstance()  throws ExceptionDBGit {
		if (ignore == null) {
			ignore = new DBGitIgnore();
		}
		return ignore;
	}
	
	public boolean matchOne(String exp) {
		for (MaskFilter mask : exclusions.values()) {
			if (mask.match(exp)) {
				return false;
			}
		}

		for (MaskFilter mask : filters.values()) {
			if (mask.match(exp)) {
				return true;
			}
		}
		return false;
	}
	
	public boolean matchSchema(String schemaName) {		
		
		for (MaskFilter mask : exclusions.values()) {
			if (mask.getMask().indexOf("/") == -1) return false;
				
			if (mask.getMask().toUpperCase().substring(0, mask.getMask().indexOf("/")).equals(schemaName.toUpperCase())) {
				return false;
			}
		}

		for (MaskFilter mask : filters.values()) {
			if (mask.getMask().indexOf("/") == -1) return false;
			if (mask.match(schemaName) || mask.getMask().toUpperCase().substring(0, mask.getMask().indexOf("/")).equals(schemaName.toUpperCase())) {
				return true;
			}
		}
		return false;
	}
}

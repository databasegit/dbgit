package ru.fusionsoft.dbgit.core;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.dbobjects.DBOptionsObject;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjOptions;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.meta.MetaSql;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

/**
 * <div class="en">Manager of meta description objects.</div>
 * <div class="ru">Менеджер объектов метаописания.</div>
 * 
 * @author mikle
 *
 */
public class GitMetaDataManager {
	private static GitMetaDataManager manager = null;
	
	protected IMapMetaObject dbObjs; 	
	protected IMapMetaObject fileObjs; 
	
	private MetaTableData currentPortion = null;
	private int currentPortionIndex = 0;
	
	protected GitMetaDataManager() {
		dbObjs = new TreeMapMetaObject();
		fileObjs = new TreeMapMetaObject();
	}
	
	public static GitMetaDataManager getInctance() {
		if (manager == null) {
			manager = new GitMetaDataManager();
		}
		return manager;
	}
	
	private void addToMapDBOptionsObject(
			IMapMetaObject objs, 
			Map<String, ? extends DBOptionsObject> map,
			DBGitMetaType type
	) throws ExceptionDBGit {
		if (map == null) return ;
		
		DBGitIgnore ignore = DBGitIgnore.getInctance(); 
		
		for (DBOptionsObject item : map.values()) {
			MetaObjOptions obj = (MetaObjOptions)MetaObjectFactory.createMetaObject(type);
			obj.setObjectOption(item);
			 
			if (ignore.matchOne(obj.getName())) continue ;
			
			objs.put(obj);
		 }
	}
	
	private void addToMapSqlObject(
			IMapMetaObject objs, 
			Map<String, ? extends DBSQLObject> map,
			DBGitMetaType type
	) throws ExceptionDBGit {
		if (map == null) return ;
		
		DBGitIgnore ignore = DBGitIgnore.getInctance(); 
		
		for (DBSQLObject item : map.values()) {
			MetaSql obj = (MetaSql)MetaObjectFactory.createMetaObject(type);
			obj.setSqlObject(item);
			
			if (ignore.matchOne(obj.getName())) continue ;
			
			objs.put(obj);
		 }
	}
	
	public boolean loadFromDB(IMetaObject obj) throws ExceptionDBGit {		
		boolean result = obj.loadFromDB();
		if (result)
			dbObjs.put(obj);
		return result;
	}
	
	public IMetaObject getCacheDBMetaObject(String name) {
	    return dbObjs.get(name);
	  }
	
	/**
	 * Get cache meta objects load from bd
	 * @return
	 */
	public IMapMetaObject getCacheDBMetaData() {
		return dbObjs;
	}
	
	/**
	 * Get cache meta objects load from files
	 * @return
	 */
	public IMapMetaObject getCacheFileMetaData() {
		return fileObjs;
	}
	
	public boolean loadNextPortion(MetaTable tbl) throws ExceptionDBGit {
		if (currentPortion == null || !tbl.getName().replace(".tbl", ".csv") .equalsIgnoreCase(currentPortion.getName()))
			currentPortionIndex = 0;
		
		ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "add", "loading") + " " + currentPortionIndex, 2);
		currentPortion = new MetaTableData(tbl.getTable());
		
		if (currentPortion != null && currentPortion.getmapRows() != null)
			currentPortion.getmapRows().clear();
				
		currentPortion.loadPortionFromDB(currentPortionIndex); 
		ConsoleWriter.println(DBGitLang.getInstance().getValue("general", "add", "size") + " " + currentPortion.getmapRows().size(), 2);

		currentPortionIndex++;
		try {
			DBGitConfig.getInstance().setValue("CURRENT_PORTION", String.valueOf(currentPortionIndex));
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
		
		return currentPortion.getmapRows().size() > 0 ? true : false;
	}
	
	public MetaTableData getCurrent() {
		return currentPortion;
	}
	
	public void setCurrentPortion(int currentPortionIndex) {
		this.currentPortionIndex = currentPortionIndex;
	}
	
	/**
	 * Load meta data from DB
	 * @return
	 */
	public IMapMetaObject loadDBMetaData() throws ExceptionDBGit {		
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		DBGitIgnore ignore = DBGitIgnore.getInctance(); 
		
		dbObjs.clear();
		Map<String, MetaTable> tbls = new HashMap<String, MetaTable>();
		
		addToMapDBOptionsObject(dbObjs, adapter.getUsers(), DBGitMetaType.DBGitUser);
		addToMapDBOptionsObject(dbObjs, adapter.getRoles(), DBGitMetaType.DBGitRole);
		//addToMapDBOptionsObject(dbObjs, adapter.getTableSpaces(), DBGitMetaType.DBGitTableSpace);
		
		Map<String, DBSchema> schemes;
		if (adapter.userHasRightsToGetDdlOfOtherUsers()) {
			schemes = adapter.getSchemes();
		} else {
			schemes = new HashMap<String, DBSchema>();			
			try {
				schemes.put(adapter.getConnection().getSchema(), new DBSchema(adapter.getConnection().getSchema()));
				ConsoleWriter.println(DBGitLang.getInstance().getValue("errors", "meta", "cantGetOtherUsersObjects"));
			} catch (SQLException e) {
				throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "meta", "cantGetCurrentSchema"));
			}
		}
		
		addToMapDBOptionsObject(dbObjs, schemes, DBGitMetaType.DBGitSchema);
		
		//load sequence
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;
			for (DBSequence seq : adapter.getSequences(schema.getName()).values()) {
				MetaSequence metaSeq = new MetaSequence();
				metaSeq.setSequence(seq);
				if (ignore.matchOne(metaSeq.getName())) continue ;
				dbObjs.put(metaSeq);
			}
			
		}
			
		//load tables
		for (DBSchema sch : schemes.values()) {
			if (ignore.matchSchema(sch.getName())) continue;
			for (DBTable tbl : adapter.getTables(sch.getName()).values()) {
				MetaTable tblMeta = new MetaTable(tbl);
				if (ignore.matchOne(tblMeta.getName())) {
					continue ;
				}
				if ( tblMeta.loadFromDB(tbl) ) {
					dbObjs.put(tblMeta);
					tbls.put(tblMeta.getName(), tblMeta);
				}
			}
		}
		
		//triggers
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;
			addToMapSqlObject(dbObjs, adapter.getTriggers(schema.getName()), DBGitMetaType.DbGitTrigger);
		}
		
		//packages
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;
			addToMapSqlObject(dbObjs, adapter.getPackages(schema.getName()), DBGitMetaType.DbGitPackage);
		}
		
		//functions
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;
			addToMapSqlObject(dbObjs, adapter.getFunctions(schema.getName()), DBGitMetaType.DbGitFunction);
		}
		
		//procedures
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;
			addToMapSqlObject(dbObjs, adapter.getProcedures(schema.getName()), DBGitMetaType.DbGitProcedure);
		}
		
		//views
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;
			addToMapSqlObject(dbObjs, adapter.getViews(schema.getName()), DBGitMetaType.DbGitView);
		}
				
		//data tables
		/*
		for (MetaTable tbl : tbls.values()) {
			MetaTableData data = new MetaTableData(tbl.getTable());
			
			if (ignore.matchOne(data.getName())) continue ;
			
			if (data.loadFromDB()) 
				dbObjs.put(data);
		}
		*/
		IMapMetaObject customObjs = adapter.loadCustomMetaObjects();
		if (customObjs != null) {
			IMapMetaObject customObjsNoIgnore = new TreeMapMetaObject();
			for (IMetaObject item : customObjs.values()) {
				if (ignore.matchOne(item.getName())) continue ;
				customObjsNoIgnore.put(item);				
			}
			dbObjs.putAll(customObjs);
		}
		
		return dbObjs;
	}
	
	public IMapMetaObject loadFileMetaData() throws ExceptionDBGit {
		return loadFileMetaData(false);
	}
	
	public IMapMetaObject loadFileMetaDataForce() throws ExceptionDBGit {
		return loadFileMetaData(true);
	}
	
	/**
	 * Load meta data from git files
	 * @return
	 */
	public IMapMetaObject loadFileMetaData(boolean force) throws ExceptionDBGit {
		try {
			IMapMetaObject objs = new TreeMapMetaObject();
			DBGit dbGit = DBGit.getInstance();  
			
			List<String> files = dbGit.getGitIndexFiles(DBGitPath.DB_GIT_PATH);
			boolean isSuccessful = true;
			
			for (int i = 0; i < files.size(); i++) {
	    		String filename = files.get(i);
	    		if (DBGitPath.isServiceFile(filename)) continue;
	    		ConsoleWriter.detailsPrint(filename + "...", 1);
	    		
	    		if (force) {			
	    			IMetaObject obj = MetaObjectFactory.createMetaObject(filename);
	    			
		    		if (obj != null) 
		    			objs.put(obj);
		    		
		    		ConsoleWriter.detailsPrintlnGreen(DBGitLang.getInstance().getValue("errors", "meta", "ok"));
	    		} else {
	    			try {		
	    				Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
		    			IMetaObject obj = loadMetaFile(filename);
		    			
		    			Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());		    			
		    			Long diff = timestampAfter.getTime() - timestampBefore.getTime();

		    			ConsoleWriter.detailsPrintlnGreen(DBGitLang.getInstance().getValue("errors", "meta", "okTime").withParams(diff.toString()));
		    			
			    		if (obj != null) {
			    			objs.put(obj);
			    		}
		    		} catch (Exception e) {
		    			isSuccessful = false;
		    			ConsoleWriter.detailsPrintlnRed(DBGitLang.getInstance().getValue("errors", "meta", "fail"));
		    			e.printStackTrace();
		    			ConsoleWriter.detailsPrintLn(e.getMessage());
		    			
		    			IMetaObject obj = MetaObjectFactory.createMetaObject(filename);
		    			
			    		if (obj != null) 
			    			objs.put(obj);
		    		}	
	    		}	    			    		
	    	}
			
			if (!isSuccessful && !force) {
				throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "meta", "invalidFiles"));
			}
			
			return objs;
		} catch(Exception e) {
			throw new ExceptionDBGit(e);
		}
	}
	
	public IMetaObject loadMetaFile(String metaName) throws ExceptionDBGit {
		AdapterFactory.createAdapter();
		if (fileObjs.containsKey(metaName))
			return fileObjs.get(metaName);		
		try {
			IMetaObject obj = MetaObjectFactory.createMetaObject(metaName);
			obj = obj.loadFromFile();
			if (obj != null) {			
				fileObjs.put(obj);
			}
			return obj;
		} catch(Exception e) {
			throw new ExceptionDBGit(e);
		}
	}
	
	/**
	 * Restore map meta object to DB
	 * @param updateObjs
	 */
	public void restoreDataBase(IMapMetaObject updateObjs) throws Exception {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		adapter.startUpdateDB();
		
		adapter.restoreDataBase(updateObjs);
				
		adapter.endUpdateDB();
		
	}
	
	/**
	 * Restore map meta object to DB
	 * @param updateObjs
	 */
	public void deleteDataBase(IMapMetaObject deleteObjs) throws Exception {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		adapter.deleteDataBase(deleteObjs);	
	}
	
	
}

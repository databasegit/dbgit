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
	private static int messageLevel = 1;

	protected GitMetaDataManager() {
		dbObjs = new TreeMapMetaObject();
		fileObjs = new TreeMapMetaObject();
	}
	
	public static GitMetaDataManager getInstance() {
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
		
		DBGitIgnore ignore = DBGitIgnore.getInstance();
		
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
		
		DBGitIgnore ignore = DBGitIgnore.getInstance();
		
		for (DBSQLObject item : map.values()) {
			MetaSql obj = (MetaSql)MetaObjectFactory.createMetaObject(type);
			obj.setSqlObject(item);
			
			if (ignore.matchOne(obj.getName())) continue ;
			
			objs.put(obj);
		 }
	}	
	
	public boolean loadFromDB(IMetaObject obj) throws ExceptionDBGit {		
		try {
			boolean result = obj.loadFromDB();
			if (result) dbObjs.put(obj);
			return result;
		} catch (ExceptionDBGitObjectNotFound ex) {
			return false;
		}
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
		
		ConsoleWriter.detailsPrintln(DBGitLang.getInstance().getValue("general", "add", "loading") + " " + currentPortionIndex + ", ", messageLevel);
		currentPortion = new MetaTableData(tbl.getTable());
		
		if (currentPortion.getmapRows() != null)
			currentPortion.getmapRows().clear();
				
		if (!currentPortion.loadPortionFromDB(currentPortionIndex)) return false;
		ConsoleWriter.detailsPrintln(DBGitLang.getInstance().getValue("general", "add", "size") + " " + currentPortion.getmapRows().size() , messageLevel);

		currentPortionIndex++;
		try {
			DBGitConfig.getInstance().setValue("CURRENT_PORTION", String.valueOf(currentPortionIndex));
		} catch (Exception e) {
			throw new ExceptionDBGit(e);
		}
		
		return currentPortion.getmapRows().size() > 0;
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
	public IMapMetaObject loadDBMetaData(boolean includeBackupSchemas) throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();

		DBGitIgnore ignore = (includeBackupSchemas)
			? new DBGitIgnore() { @Override protected void loadFileDBIgnore() throws ExceptionDBGit { loadFileDBIgnore(true); }}
			: DBGitIgnore.getInstance();

		dbObjs.clear();
		Map<String, MetaTable> tbls = new HashMap<String, MetaTable>();

		if (!ignore.matchOne("*." + DBGitMetaType.DBGitUser.getValue()))
			addToMapDBOptionsObject(dbObjs, adapter.getUsers(), DBGitMetaType.DBGitUser);

		if (!ignore.matchOne("*." + DBGitMetaType.DBGitRole.getValue()))
			addToMapDBOptionsObject(dbObjs, adapter.getRoles(), DBGitMetaType.DBGitRole);
		//addToMapDBOptionsObject(dbObjs, adapter.getTableSpaces(), DBGitMetaType.DBGitTableSpace);

		Map<String, DBSchema> schemes;
		if (adapter.userHasRightsToGetDdlOfOtherUsers()) {
			schemes = adapter.getSchemes();
		} else {
			schemes = new HashMap<String, DBSchema>();
			try {
				final DBSchema dbSchema = new DBSchema(adapter.getConnection().getSchema());
				schemes.put(adapter.getConnection().getSchema(), dbSchema);
				ConsoleWriter.println(DBGitLang.getInstance().getValue("errors", "meta", "cantGetOtherUsersObjects"), messageLevel);
			} catch (SQLException e) {
				throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "meta", "cantGetCurrentSchema").toString(), e);
			}
		}

		addToMapDBOptionsObject(dbObjs, schemes, DBGitMetaType.DBGitSchema);

		//load sequence
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DBGitSequence.getValue())) {
				for (DBSequence seq : adapter.getSequences(schema.getName()).values()) {
					MetaSequence metaSeq = new MetaSequence();
					metaSeq.setSequence(seq);
					if (ignore.matchOne(metaSeq.getName())) continue ;
					dbObjs.put(metaSeq);
				}
			}

		}


		//load tables
		for (DBSchema sch : schemes.values()) {
			if (ignore.matchSchema(sch.getName())) continue;

			if (!ignore.matchOne(sch.getName() + "/*." + DBGitMetaType.DBGitTable.getValue())) {
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
		}

		//triggers, packages, functions, procedures, views
		for (DBSchema schema : schemes.values()) {
			if (ignore.matchSchema(schema.getName())) continue;

			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DbGitTrigger.getValue()))
				addToMapSqlObject(dbObjs, adapter.getTriggers(schema.getName()), DBGitMetaType.DbGitTrigger);
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DbGitPackage.getValue()))
				addToMapSqlObject(dbObjs, adapter.getPackages(schema.getName()), DBGitMetaType.DbGitPackage);
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DbGitFunction.getValue()))
				addToMapSqlObject(dbObjs, adapter.getFunctions(schema.getName()), DBGitMetaType.DbGitFunction);
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DbGitProcedure.getValue()))
				addToMapSqlObject(dbObjs, adapter.getProcedures(schema.getName()), DBGitMetaType.DbGitProcedure);
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DbGitView.getValue()))
				addToMapSqlObject(dbObjs, adapter.getViews(schema.getName()), DBGitMetaType.DbGitView);
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DBGitEnum.getValue()))
				addToMapSqlObject(dbObjs, adapter.getEnums(schema.getName()), DBGitMetaType.DBGitEnum);
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DBGitUserDefinedType.getValue()))
				addToMapSqlObject(dbObjs, adapter.getUDTs(schema.getName()), DBGitMetaType.DBGitUserDefinedType);
			if (!ignore.matchOne(schema.getName() + "/*." + DBGitMetaType.DBGitDomain.getValue()))
				addToMapSqlObject(dbObjs, adapter.getDomains(schema.getName()), DBGitMetaType.DBGitDomain);
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

	public IMapMetaObject loadDBMetaData() throws ExceptionDBGit {		
		return loadDBMetaData(false);
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

			ConsoleWriter.println(DBGitLang.getInstance()
			    .getValue("general", "meta", "loadFiles")
			    .withParams("")
			    , messageLevel
			);

			for (int i = 0; i < files.size(); i++) {
	    		String filename = files.get(i);
	    		if (DBGitPath.isServiceFile(filename)) continue;

				ConsoleWriter.println(DBGitLang.getInstance()
					.getValue("general", "meta", "loadFile")
					.withParams(filename)
					, messageLevel+1
				);

	    		if (force) {
	    			IMetaObject obj = loadMetaFile(filename);

		    		if (obj != null) {
		    			objs.put(obj);
					}

	    		} else {
	    			try {		
		    			IMetaObject obj = loadMetaFile(filename);
		    			
			    		if (obj != null) {
			    			objs.put(obj);
			    		}
		    		} catch (Exception e) {
//		    			ConsoleWriter.printlnRed(DBGitLang.getInstance().getValue("errors", "meta", "loadMetaFile")
						final String msg = DBGitLang.getInstance()
							.getValue("errors", "meta", "cantLoadMetaFile")
							.withParams(filename);
		    			throw new ExceptionDBGit(msg, e);
//						isSuccessful = false;
//						ConsoleWriter.detailsPrintln(e.getMessage(), messageLevel);
//		    			IMetaObject obj = MetaObjectFactory.createMetaObject(filename);
//						objs.put(obj);
		    		}	
	    		}	    			    		
	    	}
			
//			if (!isSuccessful && !force) {
//				throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "meta", "invalidFiles"));
//			}
			
			return objs;
		} catch(Exception e) {
			throw new ExceptionDBGit(e);
		}
	}

	public IMetaObject loadMetaFile(String metaName) throws ExceptionDBGit {
		return loadMetaFile(metaName, true);
	}

	public IMetaObject loadMetaFile(String metaName, boolean forceLoad) throws ExceptionDBGit {
		AdapterFactory.createAdapter();
		if (!forceLoad && fileObjs.containsKey(metaName)) return fileObjs.get(metaName);
		try
		{
			IMetaObject obj = MetaObjectFactory.createMetaObject(metaName);
			obj = obj.loadFromFile();
			if (obj != null) {			
				fileObjs.put(obj);
			}
			return obj;
		} catch(Exception e) {
			throw new ExceptionDBGit(e.getLocalizedMessage(), e);
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
	 * Delete map meta object from DB and index if specified
	 * @param deleteObjs
	 * @param isDeleteFromIndex should I delete dropped object entry from dbindex?
	 */
	public void deleteDataBase(IMapMetaObject deleteObjs, boolean isDeleteFromIndex) throws Exception {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		adapter.deleteDataBase(deleteObjs, isDeleteFromIndex);
	}

	/**
	 * Delete map meta object from DB
	 * @param deleteObjs
	 */
	public void deleteDataBase(IMapMetaObject deleteObjs) throws Exception {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		adapter.deleteDataBase(deleteObjs, false);
	}

	public int removeFromGit(ItemIndex itemIndex) throws ExceptionDBGit {
		DBGit dbGit = DBGit.getInstance();
		IMetaObject dummyImo = IMetaObject.create(itemIndex.getName());
		dbGit.removeFileFromIndexGit(DBGitPath.DB_GIT_PATH+"/"+ dummyImo.getFileName());
		return 1;
	}
}

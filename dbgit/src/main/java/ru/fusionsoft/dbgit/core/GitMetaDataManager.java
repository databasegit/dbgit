package ru.fusionsoft.dbgit.core;

import java.io.IOException;
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

/**
 * <div class="en">Manager of meta description objects.</div>
 * <div class="ru">Менеджер объектов метаописания.</div>
 * 
 * @author mikle
 *
 */
public class GitMetaDataManager {
	
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
	
	/**
	 * Load meta data from DB
	 * @return
	 */
	public IMapMetaObject loadDBMetaData() throws ExceptionDBGit {		
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		DBGitIgnore ignore = DBGitIgnore.getInctance(); 
		
		IMapMetaObject objs = new TreeMapMetaObject();
		Map<String, MetaTable> tbls = new HashMap<String, MetaTable>();
		
		addToMapDBOptionsObject(objs, adapter.getUsers(), DBGitMetaType.DBGitUser);
		addToMapDBOptionsObject(objs, adapter.getRoles(), DBGitMetaType.DBGitRole);
		addToMapDBOptionsObject(objs, adapter.getTableSpaces(), DBGitMetaType.DBGitTableSpace);
		Map<String, DBSchema> schemes = adapter.getSchemes();
		addToMapDBOptionsObject(objs, schemes, DBGitMetaType.DBGitSchema);
		
		//load sequence
		for (DBSchema schema : schemes.values()) {
			for (DBSequence seq : adapter.getSequences(schema).values()) {
				MetaSequence metaSeq = new MetaSequence();
				metaSeq.setSequence(seq);
				if (ignore.matchOne(metaSeq.getName())) continue ;
				objs.put(metaSeq);
			}
			
		}
			
		//load tables
		for (DBSchema sch : schemes.values()) {
			for (DBTable tbl : adapter.getTables(sch).values()) {
				MetaTable tblMeta = new MetaTable(tbl);
				
				if (ignore.matchOne(tblMeta.getName())) continue ;
				
				tblMeta.loadFromDB(tbl);
				objs.put(tblMeta);
				tbls.put(tblMeta.getName(), tblMeta);
			}
		}
		
		//triggers
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getTriggers(schema), DBGitMetaType.DbGitTrigger);
		}
		
		//packages
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getPackages(schema), DBGitMetaType.DbGitPackage);
		}
		
		//functions
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getFunctions(schema), DBGitMetaType.DbGitFunction);
		}
		
		//procedures
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getProcedures(schema), DBGitMetaType.DbGitProcedure);
		}
		
		//views
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getViews(schema), DBGitMetaType.DbGitView);
		}
				
		//data tables
		for (MetaTable tbl : tbls.values()) {
			MetaTableData data = new MetaTableData(tbl.getTable());
			
			if (ignore.matchOne(data.getName())) continue ;
			
			data.loadFromDB();
			objs.put(data);
		}
		
		IMapMetaObject customObjs = adapter.loadCustomMetaObjects();
		if (customObjs != null) {
			IMapMetaObject customObjsNoIgnore = new TreeMapMetaObject();
			for (IMetaObject item : customObjs.values()) {
				if (ignore.matchOne(item.getName())) continue ;
				customObjsNoIgnore.put(item);				
			}
			objs.putAll(customObjs);
		}
		
		return objs;
	}
	
	/**
	 * Load meta data from git files
	 * @return
	 */
	public IMapMetaObject loadFileMetaData() throws ExceptionDBGit {
		try {
			IMapMetaObject objs = new TreeMapMetaObject();
			DBGit dbGit = DBGit.getInctance();  
			
			List<String> files = dbGit.getGitIndexFiles(DBGitPath.DB_GIT_PATH);
			for (int i = 0; i < files.size(); i++) {
	    		String filename = files.get(i);

	    		if (DBGitPath.isServiceFile(filename)) continue;
	    		
	    		IMetaObject obj = MetaObjectFactory.createMetaObject(filename);
	    		obj = obj.loadFromFile();
	    		if (obj != null)
	    			objs.put(obj);
	    	}
			
			return objs;
		} catch(IOException e) {
			throw new ExceptionDBGit(e);
		}
	}
	
	/**
	 * Restore map meta object to DB
	 * @param updateObjs
	 */
	public void restoreDataBase(IMapMetaObject updateObjs) throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		adapter.startUpdateDB();
		
		adapter.restoreDataBase(updateObjs);
				
		adapter.endUpdateDB();
		
	}
	
	/**
	 * Restore map meta object to DB
	 * @param updateObjs
	 */
	public void deleteDataBase(IMapMetaObject deleteObjs) throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		adapter.deleteDataBase(deleteObjs);	
	}
	
	
}

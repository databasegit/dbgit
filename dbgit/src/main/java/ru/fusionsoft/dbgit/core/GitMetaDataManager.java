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
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjOptions;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
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
		
		for (DBOptionsObject item : map.values()) {
			//TODO ignore dbgit
			 MetaObjOptions obj = (MetaObjOptions)MetaObjectFactory.createMetaObject(type);
			 obj.setObjectOption(item);
			 objs.put(obj.getName(), obj);
		 }
	}
	
	private void addToMapSqlObject(
			IMapMetaObject objs, 
			Map<String, ? extends DBSQLObject> map,
			DBGitMetaType type
	) throws ExceptionDBGit {
		if (map == null) return ;
		
		for (DBSQLObject item : map.values()) {
			//TODO ignore dbgit
			MetaSql obj = (MetaSql)MetaObjectFactory.createMetaObject(type);
			obj.setSqlObject(item);
			objs.put(obj.getName(), obj);
		 }
	}
	
	/**
	 * Load meta data from DB
	 * @return
	 */
	public IMapMetaObject loadDBMetaData() throws ExceptionDBGit {		
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		IMapMetaObject objs = new TreeMapMetaObject();
		Map<String, MetaTable> tbls = new HashMap<String, MetaTable>();
		
		addToMapDBOptionsObject(objs, adapter.getUsers(), DBGitMetaType.DBGitUser);
		addToMapDBOptionsObject(objs, adapter.getRoles(), DBGitMetaType.DBGitRole);
		addToMapDBOptionsObject(objs, adapter.getTableSpaces(), DBGitMetaType.DBGitTableSpace);
		Map<String, DBSchema> schemes = adapter.getSchemes();
		addToMapDBOptionsObject(objs, schemes, DBGitMetaType.DBGitSchema);
		
		//load sequence
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getSequences(schema), DBGitMetaType.DBGitSchema);
		}
			
		//load tables
		for (DBSchema sch : schemes.values()) {
			for (DBTable tbl : adapter.getTables(sch).values()) {
				//TODO ignore dbgit
				MetaTable tblMeta = new MetaTable(tbl);
				tblMeta.loadFromDB();
				objs.put(tblMeta.getName(), tblMeta);
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
			//TODO ignore dbgit
			MetaTableData data = new MetaTableData(tbl.getTable());
			data.loadFromDB();
			objs.put(data.getName(), data);
		}
		
		IMapMetaObject customObjs = adapter.loadCustomMetaObjects();
		if (customObjs != null) {
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
	    		objs.put(obj.getName(), obj);
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
	
	
}

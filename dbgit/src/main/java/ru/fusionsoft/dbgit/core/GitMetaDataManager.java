package ru.fusionsoft.dbgit.core;

import java.util.HashMap;
import java.util.Map;

import javax.xml.validation.Schema;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.dbobjects.DBOptionsObject;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBUser;
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
			addToMapSqlObject(objs, adapter.getPackages(schema), DBGitMetaType.DbGitTrigger);
		}
		
		//functions
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getFunctions(schema), DBGitMetaType.DbGitTrigger);
		}
		
		//procedures
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getProcedures(schema), DBGitMetaType.DbGitTrigger);
		}
		
		//views
		for (DBSchema schema : schemes.values()) {
			addToMapSqlObject(objs, adapter.getViews(schema), DBGitMetaType.DbGitTrigger);
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
	public IMapMetaObject loadFileMetaData() {
		IMapMetaObject objs = new TreeMapMetaObject();
		//scan files and load object memory
		return objs;
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

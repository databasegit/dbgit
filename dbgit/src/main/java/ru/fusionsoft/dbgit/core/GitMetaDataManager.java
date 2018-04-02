package ru.fusionsoft.dbgit.core;

import java.util.Map;

import javax.xml.validation.Schema;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;

/**
 * <div class="en">Manager of meta description objects.</div>
 * <div class="ru">Менеджер объектов метаописания.</div>
 * 
 * @author mikle
 *
 */
public class GitMetaDataManager {
	
	/**
	 * Load meta data from DB
	 * @return
	 */
	public IMapMetaObject loadDBMetaData() {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		IMapMetaObject objs = new TreeMapMetaObject();
		
		//load MetaDBInfo
		
		//load sequence
		

		//load table
		for (DBSchema sch : adapter.getSchemes().values()) {
			for (DBTable tbl : adapter.getTables(sch).values()) {
				IMetaObject tblMeta = new MetaTable(tbl);
				tblMeta.loadFromDB();
								
			}
		}
		
		//load views
		
		//load code
		
		//merge with adapter.loadCustomMetaObjects();
		
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
	public void restoreDataBase(IMapMetaObject updateObjs) {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		adapter.startUpdateDB();
		
		adapter.restoreDataBase(updateObjs);
				
		adapter.endUpdateDB();
		
	}
	
	
}

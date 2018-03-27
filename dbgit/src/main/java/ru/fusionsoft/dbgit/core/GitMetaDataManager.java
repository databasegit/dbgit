package ru.fusionsoft.dbgit.core;

import java.util.Map;

import javax.xml.validation.Schema;
import javax.xml.validation.meta.IMapMetaObject;
import javax.xml.validation.meta.IMetaObject;
import javax.xml.validation.meta.MetaTable;
import javax.xml.validation.meta.TreeMapMetaObject;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBTable;

public class GitMetaDataManager {
	
	public IMapMetaObject loadDBMetaData() {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		IMapMetaObject objs = new TreeMapMetaObject();
		
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
		
		return objs;
	}
	
	
	public IMapMetaObject loadFileMetaData() {
		IMapMetaObject objs = new TreeMapMetaObject();
		//scan files and load object memory
		return objs;
	}
	
	
	public void restoreDataBase(IMapMetaObject updateObjs) {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		adapter.startUpdateDB();
		
		adapter.restoreDataBase(updateObjs);
				
		adapter.endUpdateDB();
		
	}
	
	
}

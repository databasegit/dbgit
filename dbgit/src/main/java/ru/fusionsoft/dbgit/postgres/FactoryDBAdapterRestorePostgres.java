package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.validation.meta.DBGitMetaType;

import ru.fusionsoft.dbgit.adapters.DBRestoreMetaSql;
import ru.fusionsoft.dbgit.adapters.IDBAdapterRestoreMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;


public class FactoryDBAdapterRestorePostgres implements IFactoryDBAdapterRestoteMetaData {	
	
	private static final Map<String, IDBAdapterRestoreMetaData> restoreAdapters;
	static {
        Map<String, IDBAdapterRestoreMetaData> aMap = new HashMap<String, IDBAdapterRestoreMetaData>();
        aMap.put(DBGitMetaType.DBGitSequence.getValue(), new DBRestoreMetaSql());
        aMap.put(DBGitMetaType.DBGitTable.getValue(), new DBRestoreTablePostgres());
        aMap.put(DBGitMetaType.DbGitProcedure.getValue(), new DBRestoreMetaSql());
        aMap.put(DBGitMetaType.DbGitFunction.getValue(), new DBRestoreMetaSql());
        aMap.put(DBGitMetaType.DbGitTrigger.getValue(), new DBRestoreMetaSql());
        aMap.put(DBGitMetaType.DbGitView.getValue(), new DBRestoreMetaSql());
        
        //TODO other restore adapter

        restoreAdapters = Collections.unmodifiableMap(aMap);
	}
	
	@Override
	public IDBAdapterRestoreMetaData getAdapterRestore(DBGitMetaType tp, Connection conn) {
		IDBAdapterRestoreMetaData re = restoreAdapters.get(tp.getValue());
		re.setConnection(conn);
		return re;
	}

}

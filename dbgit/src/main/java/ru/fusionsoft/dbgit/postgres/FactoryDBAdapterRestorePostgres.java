package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreMetaNotSupport;
import ru.fusionsoft.dbgit.adapters.DBRestoreMetaSql;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapterRestoreMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IDBGitMetaType;


public class FactoryDBAdapterRestorePostgres implements IFactoryDBAdapterRestoteMetaData {	
	
	private static final Map<String, IDBAdapterRestoreMetaData> restoreAdapters;
	static {
        Map<String, IDBAdapterRestoreMetaData> aMap = new HashMap<String, IDBAdapterRestoreMetaData>();
        aMap.put(DBGitMetaType.DBGitSchema.getValue(), new DBRestoreSchemaPostgres());
        aMap.put(DBGitMetaType.DBGitTableSpace.getValue(), new DBRestoreTableSpacePostgres());
        aMap.put(DBGitMetaType.DBGitRole.getValue(), new DBRestoreRolePostgres());
        aMap.put(DBGitMetaType.DBGitSequence.getValue(), new DBRestoreSequencePostgres());
        aMap.put(DBGitMetaType.DBGitTable.getValue(), new DBRestoreTablePostgres());
        aMap.put(DBGitMetaType.DbGitTableData.getValue(), new DBRestoreTableDataPostgres());
        aMap.put(DBGitMetaType.DbGitProcedure.getValue(), new DBRestoreProcedurePostgres());
        aMap.put(DBGitMetaType.DbGitFunction.getValue(), new DBRestoreFunctionPostgres());
        aMap.put(DBGitMetaType.DbGitTrigger.getValue(), new DBRestoreTriggerPostgres());
        aMap.put(DBGitMetaType.DbGitView.getValue(), new DBRestoreViewPostgres());
        //aMap.put(DBGitMetaType.DBGitUser.getValue(), new DBRestoreUserPostgres());


        
        //TODO other restore adapter

        restoreAdapters = Collections.unmodifiableMap(aMap);
	}
	
	@Override
	public IDBAdapterRestoreMetaData getAdapterRestore(IDBGitMetaType tp, IDBAdapter adapter) {		
		if (!restoreAdapters.containsKey(tp.getValue())) {
			return new DBRestoreMetaNotSupport();
		}
		
		IDBAdapterRestoreMetaData re = restoreAdapters.get(tp.getValue());
		re.setAdapter(adapter);
		return re;
	}

}

package ru.fusionsoft.dbgit.oracle;

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

public class FactoryDBAdapterRestoreOracle implements IFactoryDBAdapterRestoteMetaData {
	
	private static final Map<String, IDBAdapterRestoreMetaData> restoreAdapters;
	static {
        Map<String, IDBAdapterRestoreMetaData> aMap = new HashMap<String, IDBAdapterRestoreMetaData>();
        aMap.put(DBGitMetaType.DBGitSchema.getValue(), new DBRestoreSchemaOracle());
        //aMap.put(DBGitMetaType.DBGitTableSpace.getValue(), new DBRestoreTableSpaceOracle());
        aMap.put(DBGitMetaType.DBGitRole.getValue(), new DBRestoreRoleOracle());
        aMap.put(DBGitMetaType.DBGitSequence.getValue(), new DBRestoreSequenceOracle());
        aMap.put(DBGitMetaType.DBGitTable.getValue(), new DBRestoreTableOracle());
        //aMap.put(DBGitMetaType.DbGitTableData.getValue(), new DBRestoreTableDataOracle());
        aMap.put(DBGitMetaType.DbGitProcedure.getValue(), new DBRestoreProcedureOracle());
        aMap.put(DBGitMetaType.DbGitFunction.getValue(), new DBRestoreFunctionOracle());
        aMap.put(DBGitMetaType.DbGitTrigger.getValue(), new DBRestoreTriggerOracle());
        aMap.put(DBGitMetaType.DbGitView.getValue(), new DBRestoreViewOracle());
        //aMap.put(DBGitMetaType.DBGitUser.getValue(), new DBRestoreUserOracle());
        
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

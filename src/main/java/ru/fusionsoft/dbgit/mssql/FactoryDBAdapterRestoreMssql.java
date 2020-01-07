package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapterRestoreMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IDBGitMetaType;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class FactoryDBAdapterRestoreMssql implements IFactoryDBAdapterRestoteMetaData {

	private static final Map<String, IDBAdapterRestoreMetaData> restoreAdapters;
	static {
		Map<String, IDBAdapterRestoreMetaData> aMap = new HashMap<String, IDBAdapterRestoreMetaData>();
		aMap.put(DBGitMetaType.DBGitSchema.getValue(), new DBRestoreSchemaMssql());
		aMap.put(DBGitMetaType.DBGitTableSpace.getValue(), new DBRestoreTableSpaceMssql());
		aMap.put(DBGitMetaType.DBGitRole.getValue(), new DBRestoreRoleMssql());
		aMap.put(DBGitMetaType.DBGitSequence.getValue(), new DBRestoreSequenceMssql());
		aMap.put(DBGitMetaType.DBGitTable.getValue(), new DBRestoreTableMssql());
		aMap.put(DBGitMetaType.DbGitTableData.getValue(), new DBRestoreTableDataMssql());
		aMap.put(DBGitMetaType.DbGitProcedure.getValue(), new DBRestoreProcedureMssql());
		aMap.put(DBGitMetaType.DbGitFunction.getValue(), new DBRestoreFunctionMssql());
		aMap.put(DBGitMetaType.DbGitTrigger.getValue(), new DBRestoreTriggerMssql());
		aMap.put(DBGitMetaType.DbGitView.getValue(), new DBRestoreViewMssql());
		aMap.put(DBGitMetaType.DBGitUser.getValue(), new DBRestoreUserMssql());



		//TODO other restore adapter

		restoreAdapters = Collections.unmodifiableMap(aMap);
	}

	@Override
	public IDBAdapterRestoreMetaData getAdapterRestore(IDBGitMetaType tp, IDBAdapter adapter) {
		if (!restoreAdapters.containsKey(tp.getValue())) {
			//return new DBRestoreMetaNotSupport();
			ConsoleWriter.println(DBGitLang.getInstance().getValue("errors", "restore", "cannotRestore").withParams(tp.getValue()));
			return null;
		}

		IDBAdapterRestoreMetaData re = restoreAdapters.get(tp.getValue());
		re.setAdapter(adapter);
		return re;
	}

}

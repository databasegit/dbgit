package ru.fusionsoft.dbgit.mysql;

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

public class FactoryDBRestoreAdapterMySql implements IFactoryDBAdapterRestoteMetaData {
    private static final Map<String, IDBAdapterRestoreMetaData> restoreAdapters;
    static {
        Map<String, IDBAdapterRestoreMetaData> aMap = new HashMap<String, IDBAdapterRestoreMetaData>();
        aMap.put(DBGitMetaType.DBGitSchema.getValue(), new DBRestoreSchemaMySql());
        //aMap.put(DBGitMetaType.DBGitTableSpace.getValue(), new DBRestoreTableSpaceMySql());
        //aMap.put(DBGitMetaType.DBGitRole.getValue(), new DBRestoreRoleMySql());
        aMap.put(DBGitMetaType.DBGitTable.getValue(), new DBRestoreTableMySql());
        aMap.put(DBGitMetaType.DbGitTableData.getValue(), new DBRestoreTableDataMySql());
        //aMap.put(DBGitMetaType.DbGitProcedure.getValue(), new DBRestoreProcedureMySql());
        //aMap.put(DBGitMetaType.DbGitFunction.getValue(), new DBRestoreFunctionMySql());
        //aMap.put(DBGitMetaType.DbGitTrigger.getValue(), new DBRestoreTriggerMySql());
        //aMap.put(DBGitMetaType.DbGitView.getValue(), new DBRestoreViewMySql());//FIXME: permanently disabled
        aMap.put(DBGitMetaType.DBGitUser.getValue(), new DBRestoreUserMySql());



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

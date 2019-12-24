package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBBackupAdapterMySql extends DBBackupAdapter {
    @Override
    public IMetaObject backupDBObject(IMetaObject obj) throws Exception {
        //TODO: change
        return null;
    }

    @Override
    public void restoreDBObject(IMetaObject obj) throws Exception {
        //TODO: change
    }

    @Override
    public boolean createSchema(StatementLogging stLog, String schema) {
        //TODO: change
        return false;
    }

    @Override
    public boolean isExists(String owner, String objectName) throws Exception {
        //TODO: change
        return false;
    }
}

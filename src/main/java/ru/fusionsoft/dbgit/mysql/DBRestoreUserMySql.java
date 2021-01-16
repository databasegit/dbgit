package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaUser;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;

public class DBRestoreUserMySql extends DBRestoreAdapter {
    @Override
    public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        try {
            if (obj instanceof MetaUser) {
                MetaUser usr = (MetaUser)obj;
                st.execute("CREATE USER "+usr.getObjectOption().getName());
            }
            else
            {
                ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
            }
        } catch (Exception e) {
            ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
        } finally {
            ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
            st.close();
        }
        return true;
    }

    @Override
    public void removeMetaObject(IMetaObject obj) throws Exception {
        // TODO Auto-generated method stub

    }
}

package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaView;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;

public class DBRestoreViewMySql extends DBRestoreAdapter {

    @Override
    public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        try {
            if (obj instanceof MetaView) {
                MetaView restoreView = (MetaView)obj;
                String ddl = restoreView.getSqlObject().getSql();
                st.execute(ddl);
                //connect.commit();//FIXME ????
            } else {
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                    ,  "view", obj.getType().getValue()
                ));
            }
        } catch (Exception e) {
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

package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBBackupAdapterMySql extends DBBackupAdapter {
    @Override
    public IMetaObject backupDBObject(IMetaObject obj) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void restoreDBObject(IMetaObject obj) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean createSchema(StatementLogging stLog, String schema) {
        try {
            Statement st = 	adapter.getConnection().createStatement();
            ResultSet rs = st.executeQuery("select count(*) cnt from information_schema.schemata where upper(schema_name) = '" +
                    PREFIX + schema.toUpperCase() + "'");

            rs.next();
            if (rs.getInt("cnt") == 0) {
                ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "creatingSchema").withParams(PREFIX + schema));
                stLog.execute("create schema " + PREFIX + schema);
            }

            rs.close();
            st.close();

            return true;
        } catch (SQLException e) {
            ConsoleWriter.println(lang.getValue("errors", "backup", "cannotCreateSchema").withParams(e.getLocalizedMessage()));
            return false;
        }
    }

    @Override
    public boolean isExists(String owner, String objectName) throws Exception {
        // TODO Auto-generated method stub
        return false;
    }
}

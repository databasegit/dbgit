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

    private String getFullDbName(String schema, String objectName) {
        if (isSaveToSchema())
            return PREFIX + schema + "." + objectName;
        else
            return schema + "." + PREFIX + objectName;
    }

    private void dropIfExists(String owner, String objectName, StatementLogging stLog) throws Exception {
        Statement st = 	adapter.getConnection().createStatement();
        ResultSet rs = st.executeQuery("select * from (\r\n" +
                "	SELECT 'TABLE' tp, table_name obj_name, table_schema sch FROM information_schema.tables \r\n" +
                "	union select 'VIEW' tp, table_name obj_name, table_schema sch from information_schema.views\r\n" +
                "	union select 'TRIGGER' tp, trigger_name obj_name, trigger_schema sch from information_schema.triggers\r\n" +
                "	union select 'FUNCTION' tp, routine_name obj_name, routine_schema sch from information_schema.routines\r\n" +
                ") all_objects\r\n" +
                "where sch = '" + owner.toLowerCase() + "' and obj_name = '" + objectName.toLowerCase() + "'");

        while (rs.next()) {
            stLog.execute("drop " + rs.getString("tp") + " " + owner + "." + objectName);
        }

        rs.close();
        st.close();
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
        Statement st = 	adapter.getConnection().createStatement();
        ResultSet rs = st.executeQuery("select count(*) cnt from (\r\n" +
                "	SELECT 'TABLE' tp, table_name obj_name, table_schema sch FROM information_schema.tables \r\n" +
                "	union select 'VIEW' tp, table_name obj_name, table_schema sch from information_schema.views\r\n" +
                "	union select 'TRIGGER' tp, trigger_name obj_name, trigger_schema sch from information_schema.triggers\r\n" +
                "	union select 'FUNCTION' tp, routine_name obj_name, routine_schema sch from information_schema.routines\r\n" +
                ") all_objects\r\n" +
                "where lower(sch) = '" + owner.toLowerCase() + "' and lower(obj_name) = '" + objectName.toLowerCase() + "'");

        rs.next();
        return rs.getInt("cnt") > 0;
    }
}

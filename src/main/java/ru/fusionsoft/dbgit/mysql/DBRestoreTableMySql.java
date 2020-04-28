package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class DBRestoreTableMySql extends DBRestoreAdapter {

    @Override
    public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        switch(step) {
            case 0:
                restoreTableMySql(obj);
                break;
            case 1:
                //restoreTableIndexesMySql(obj);//FIXME
                break;
            case -1:
                //restoreTableConstraintMySql(obj);//FIXME
                break;
            default:
                return true;
        }
        return false;
    }

    public void removeMetaObject(IMetaObject obj) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        try {
            MetaTable tblMeta = (MetaTable)obj;
            DBTable tbl = tblMeta.getTable();
            if (tbl == null)
                return;
            String schema = getPhisicalSchema(tbl.getSchema());
            schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
            st.execute("drop table if exists " + schema + "." + tbl.getName());
        } catch (Exception e) {
            ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
        } finally {
            st.close();
        }
    }

    public void restoreTableMySql(IMetaObject obj) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        try {
            if (obj instanceof MetaTable) {
                MetaTable restoreTable = (MetaTable) obj;
                String schema = getPhisicalSchema(restoreTable.getTable().getSchema().toLowerCase());
                schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
                String tblName = schema + ".`" + restoreTable.getTable().getName() + "`";
                ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "table").withParams(tblName) + "\n", 1);
                ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "createTable"), 2);
                StringBuilder pk = new StringBuilder();
                String ddl = "CREATE TABLE IF NOT EXISTS " + tblName + " ("
                        + restoreTable
                        .getFields()
                        .values()
                        .stream()
                        .sorted(Comparator.comparing(DBTableField::getOrder))
                        .map(f -> {
                            if (f.getIsPrimaryKey())
                                pk.append(f.getName() + ",");
                            return "`" + f.getName() + "` "
                                            + f.getTypeSQL() + (f.getFixed() ? "(" + f.getLength() + ")" : "")
                                            + (f.getIsNullable() ? "" : " NOT NULL")
                                            + (f.getDefaultValue() == null ? "" : " default " + f.getDefaultValue())
                                            + (f.getDescription() == null ? "" : " comment '" + f.getDescription() + "'");
                                }
                        ).collect(Collectors.joining(", "))
                        + (pk.length() > 1 ? ", PRIMARY KEY (" + pk.replace(pk.length() - 1, pk.length(), ")").toString() : "")
                        //FIXME: add foreign keys
                        + ");";
                st.execute(ddl);
                ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
            } else {
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
            }
        } catch (Exception e) {
            ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
        } finally {
            st.close();
        }
    }
}

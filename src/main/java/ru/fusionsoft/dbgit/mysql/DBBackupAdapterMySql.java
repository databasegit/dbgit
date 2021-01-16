package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.NameMeta;
import ru.fusionsoft.dbgit.postgres.DBAdapterPostgres;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.meta.MetaSql;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

public class DBBackupAdapterMySql extends DBBackupAdapter {
    @Override
    public IMetaObject backupDBObject(IMetaObject obj) throws Exception {
        Connection connection = adapter.getConnection();
        StatementLogging stLog = new StatementLogging(connection, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        try {
            if (obj instanceof MetaSql) {
                MetaSql metaSql = (MetaSql) obj;
                String objectName = metaSql.getSqlObject().getName();
                metaSql.loadFromDB();
                String ddl = metaSql.getSqlObject().getSql();
                String schema = metaSql.getSqlObject().getSchema();
                if (isSaveToSchema()) {
                    createSchema(stLog, schema);
                }
                ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 3);
                ////dropIfExists(isSaveToSchema() ? PREFIX + schema : schema,
                ////		isSaveToSchema() ? objectName : PREFIX + objectName, stLog);
                //ddl = ddl.replace(schema + "." + objectName, getFullDbName(schema, objectName));
                ////ddl += "alter table "+ tableName + " owner to "+ metaTable.getTable().getOptions().get("tableowner").getData()+";\n";
                //stLog.execute(ddl);
                File file = new File(DBGitPath.getFullPath() + metaSql.getFileName());
                if (file.exists())
                    obj = metaSql.loadFromFile();
                ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
            } else if (obj instanceof MetaTable) {
                MetaTable metaTable = (MetaTable) obj;
                metaTable.loadFromDB();
                String tableName = metaTable.getTable().getName();
                String schema = metaTable.getTable().getSchema();
                schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
                //String backupTableSam = getFullDbName(schema, tableName);
                //String backupTableSamRe = Matcher.quoteReplacement(backupTableSam);
                //String tableSamRe = schema + "\\.\\\"?" + Pattern.quote(tableName) + "\\\"?";
                if(!isExists(schema, tableName)) {
                    File file = new File(DBGitPath.getFullPath() + metaTable.getFileName());
                    if (file.exists())
                        obj = metaTable.loadFromFile();
                    return obj;
                }
                if (isSaveToSchema()) {
                    createSchema(stLog, schema);
                }
                ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "tryingToCopy").withParams(tableName, getFullDbName(schema, tableName)), 3);
                //dropIfExists(
                        //isSaveToSchema() ? PREFIX + schema : schema,
                        //isSaveToSchema() ? tableName : PREFIX + tableName, stLog
                //);
                //StringBuilder tableDdl = new StringBuilder(MessageFormat.format(
                        //"create table {0} as (select * from {1}.{2} where 1={3}) {4};\n alter table {0} owner to {5};\n"
                        //, backupTableSam
                        //, schema
                        //, DBAdapterPostgres.escapeNameIfNeeded(tableName)
                        //, isToSaveData() ? "1" : "0"
                        //, metaTable.getTable().getOptions().getChildren().containsKey("tablespace")
                                //? " tablespace " + metaTable.getTable().getOptions().get("tablespace").getData()
                                //: ""
                        //, metaTable.getTable().getOptions().get("owner").getData()
                //));
                //for (DBIndex index : metaTable.getIndexes().values()) {
                    //String indexName = index.getName();
                    //String indexNameRe = "\\\"?" + Pattern.quote(indexName) + "\\\"?";
                    //String backupIndexNameRe = Matcher.quoteReplacement(DBAdapterPostgres.escapeNameIfNeeded(PREFIX + indexName));
                    //String indexDdl = MessageFormat.format(
                            //"{0} {1};\n"
                            //, index.getSql().replaceAll(indexNameRe, backupIndexNameRe).replaceAll(tableSamRe, backupTableSamRe)
                            //, metaTable.getTable().getOptions().getChildren().containsKey("tablespace")
                                    //?  " tablespace "+index.getOptions().get("tablespace").getData()
                                    //: ""
                    //);
                    //if (indexDdl.length() > 3) { tableDdl.append(indexDdl); }
                //}
                //for (DBConstraint constraint : metaTable.getConstraints().values()) {
                    //String name = DBAdapterPostgres.escapeNameIfNeeded(PREFIX + constraint.getName());
                    //String constrDdl = MessageFormat.format(
                            //"alter table {0} add {1};\n"
                            //,backupTableSam
                            //,metaTable.getIndexes().containsKey(constraint.getName())
                                    //? "primary key using index " + name
                                    //: "constraint " + name + " " + constraint.getSql()
                    //);
                    //if (constrDdl.length() > 3) { tableDdl.append(constrDdl); }
                //}
                //stLog.execute(tableDdl.toString());
                File file = new File(DBGitPath.getFullPath() + metaTable.getFileName());
                if (file.exists())
                    obj = metaTable.loadFromFile();
                ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
            } else if (obj instanceof MetaSequence) {
                MetaSequence metaSequence = (MetaSequence) obj;
                metaSequence.loadFromDB();
                String objectName = metaSequence.getSequence().getName();
                String schema = metaSequence.getSequence().getSchema();
                if (isSaveToSchema()) {
                    createSchema(stLog, schema);
                }
                String sequenceName = getFullDbName(schema, objectName);
                ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 3);
                //String ddl = "create sequence " + sequenceName + "\n"
                        //+ (metaSequence.getSequence().getOptions().get("cycle_option").toString().equals("YES") ? "CYCLE\n" : "")
                        //+ " INCREMENT " + metaSequence.getSequence().getOptions().get("increment").toString() + "\n"
                        //+ " START " + metaSequence.getSequence().getOptions().get("start_value").toString() + "\n"
                        //+ " MINVALUE " + metaSequence.getSequence().getOptions().get("minimum_value").toString() + "\n"
                        //+ " MAXVALUE " + metaSequence.getSequence().getOptions().get("maximum_value").toString() + ";\n";
                //ddl += "alter sequence "+ sequenceName + " owner to "+ metaSequence.getSequence().getOptions().get("owner").getData()+";\n";
                //dropIfExists(
                        //isSaveToSchema() ? PREFIX + schema : schema,
                        //isSaveToSchema() ? objectName : PREFIX + objectName, stLog
                //);
                //stLog.execute(ddl);
                File file = new File(DBGitPath.getFullPath() + metaSequence.getFileName());
                if (file.exists())
                    obj = metaSequence.loadFromFile();
                ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
            }
        } catch (SQLException e1) {
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").
                    withParams(obj.getName() + ": " + e1.getLocalizedMessage()));
        } catch (Exception e) {
            ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
            connection.rollback();
            throw new ExceptionDBGitRestore(lang.getValue("errors", "backup", "backupError").withParams(obj.getName()), e);
        } finally {
            connection.commit();
            stLog.close();
        }
        return obj;
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

    public void dropIfExists(String owner, String objectName, StatementLogging stLog) throws SQLException {
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
    public void dropIfExists(IMetaObject imo, StatementLogging stLog) throws SQLException {

        NameMeta nm = new NameMeta(imo);
        DBGitMetaType type = (DBGitMetaType) nm.getType();

        Statement st = 	adapter.getConnection().createStatement();
        ResultSet rs = st.executeQuery(MessageFormat.format("select * from (\r\n" +
                    "	SELECT 'TABLE' tp, table_name obj_name, table_schema sch FROM information_schema.tables WHERE 1={0}\r\n" +
                    "	union select 'VIEW' tp, table_name obj_name, table_schema sch from information_schema.views WHERE 1={1}\r\n" +
                    "	union select 'TRIGGER' tp, trigger_name obj_name, trigger_schema sch from information_schema.triggers WHERE 1={2}\r\n" +
                    "	union select 'FUNCTION' tp, routine_name obj_name, routine_schema sch from information_schema.routines WHERE 1={3}\r\n" +
                    ") all_objects\r\n" +
                    "where sch = '{4}' and obj_name = '{5}'",
                type.equals(DBGitMetaType.DBGitTable) ? "1" : "0",
                type.equals(DBGitMetaType.DbGitView) ? "1" : "0",
                type.equals(DBGitMetaType.DbGitTrigger) ? "1" : "0",
                type.equals(DBGitMetaType.DbGitFunction) ? "1" : "0",
                nm.getSchema(), nm.getName()
        ));

        while (rs.next()) {
            stLog.execute(MessageFormat.format("DROP {0} {1}.{2}",
                rs.getString("tp"),
                nm.getSchema(),
                adapter.escapeNameIfNeeded(nm.getName()))
            );
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
                ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "creatingSchema").withParams(PREFIX + schema), 3);
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
    public boolean isExists(String owner, String objectName) throws SQLException {
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

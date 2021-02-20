package ru.fusionsoft.dbgit.mysql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DBRestoreTableMySql extends DBRestoreAdapter {

    @Override
    public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        switch(step) {
            case 0:
                restoreTableMySql(obj);
                break;
            case 1:
                //restoreTableIndexesMySql(obj);//FIXME: permanently disabled
                break;
            case -1:
                //restoreTableConstraintMySql(obj);
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
            ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()), 0);
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
        } finally {
            st.close();
        }
    }

    public void restoreTableMySql(IMetaObject obj) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();

        try (StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());){
            if (obj instanceof MetaTable) {
                MetaTable restoreTable = (MetaTable) obj;
                String schema = getPhisicalSchema(restoreTable.getTable().getSchema().toLowerCase());
                schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
                String tblName = schema + ".`" + restoreTable.getTable().getName() + "`";
                ConsoleWriter.detailsPrintln(lang.getValue("general", "restore", "createTable"), messageLevel);
                StringBuilder pk = new StringBuilder();
                String ddl = "CREATE TABLE IF NOT EXISTS " + tblName + " ("
                        + restoreTable
                        .getFields()
                        .values()
                        .stream()
                        .sorted(Comparator.comparing(DBTableField::getOrder))
                        .map(f -> {
                            if (f.getIsPrimaryKey())
                                pk.append(f.getName() + "`, `");
                            return "`" + f.getName() + "` "
                                    + f.getTypeSQL() + (f.getFixed() ? "(" + f.getLength() + ")" : "")
                                    + (f.getIsNullable() ? "" : " NOT NULL")
                                    + (f.getDefaultValue() == null ? "" :
                                            (f.getDefaultValue().toLowerCase().contains("nextval(") ? " AUTO_INCREMENT" : ""))
                                    //+ (f.getDefaultValue() == null ? "" : " default " + f.getDefaultValue())
                                    + (f.getDescription() == null ? "" : " comment '" + f.getDescription() + "'");
                                }
                        ).collect(Collectors.joining(", "))
                        + (pk.length() > 1 ? ", PRIMARY KEY (`" + pk.replace(pk.length() - 4, pk.length(), "").toString() + "`)" : "")
                        //FIXME: add foreign keys
                        + ");";
                st.execute(ddl);
                ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
            } else {
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                    ,  "table", obj.getType().getValue()
                ));
            }
        } catch (Exception e) {
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
        }
    }

    public void restoreTableIndexesMySql(IMetaObject obj) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        ConsoleWriter.detailsPrintln(lang.getValue("general", "restore", "restoreIndex").withParams(obj.getName()), messageLevel);
        try {
            if (obj instanceof MetaTable) {
                MetaTable restoreTable = (MetaTable)obj;
                String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
                schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
                String tableName = restoreTable.getTable().getName();

                DBTable existTable = adapter.getTable(schema, tableName);
                Pattern indexFields = Pattern.compile("\\(.+\\)");//FIXME ???
                if(existTable != null) {
                    for (DBIndex index : restoreTable.getIndexes().values()) {
                        Matcher matcher = indexFields.matcher(index.getSql());
                        if(matcher.find()) {
                            String ddl = "CREATE INDEX `" + index.getName() + "` ON " + schema + ".`" + tableName + "`";
                            ddl += matcher.group().replaceAll("\"", "`").replaceAll("\\`", "\\\"");
                            st.execute(ddl);
                        }
                    }
                    //FIXME: ??? V
                    //Map<String, DBIndex> currentIndexes = adapter.getIndexes(schema, currentTable.getName());
                    //MapDifference<String, DBIndex> diffInd = Maps.difference(restoreTable.getIndexes(), currentIndexes);
                    //Map<String, DBIndex> restoringIdxsUnique = diffInd.entriesOnlyOnLeft();
                    //Map<String, DBIndex> existingIdxsUnique = diffInd.entriesOnlyOnRight();
                    //Map<String, ValueDifference<DBIndex>> mergingIdxs = diffInd.entriesDiffering();
                    ////restore missing
                    //if(!restoringIdxsUnique.isEmpty()) {
                    //for(DBIndex ind : restoringIdxsUnique.values()) {
                    //st.execute(ind.getSql());
                    //}
                    //}
                    ////drop not matched
                    //if(!existingIdxsUnique.isEmpty()) {
                    //for(DBIndex ind:existingIdxsUnique.values()) {
                    //st.execute("DROP INDEX "+schema+"."+ind.getName());
                    //}
                    //}
                    ////process intersects
                    //if(!mergingIdxs.isEmpty()) {
                    //for(ValueDifference<DBIndex> idx : mergingIdxs.values()) {
                    ////so just drop and create again
                    //String ddl;
                    //if(idx.rightValue().getOptions().get("is_unique").getData().equals("0")){
                    //ddl = MessageFormat.format(
                    //"DROP INDEX [{2}] ON [{0}].[{1}] ",
                    //schema, currentTable.getName(), idx.rightValue().getName()
                    //);
                    //}
                    //else{
                    //ddl = MessageFormat.format(
                    //"ALTER TABLE [{0}].[{1}] DROP CONSTRAINT [{2}]",
                    //schema, currentTable.getName(), idx.rightValue().getName()
                    //);
                    //}
                    //st.execute(ddl);
                    //st.execute(idx.leftValue().getSql());
                    //}
                    //}
                } else {
                    String errText = lang.getValue("errors", "meta", "notFound").withParams(obj.getName());
                    throw new ExceptionDBGitRestore(errText);
                }
            } else {
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                        ,  "table", obj.getType().getValue()
                ));
            }
        } catch (Exception e) {
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
        } finally {
            ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
            st.close();
        }
    }

    private void restoreTableConstraintMySql(IMetaObject obj) throws Exception {
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        ConsoleWriter.detailsPrintln(lang.getValue("general", "restore", "restoreConstr").withParams(obj.getName()), messageLevel);
        try {
            if (obj instanceof MetaTable) {
                MetaTable restoreTable = (MetaTable)obj;
                String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
                schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
                String tableName = restoreTable.getTable().getName();
                //List<DBConstraint> restoredConstraints = restoreTable.getConstraints().values()
                        //.stream()
                        //.sorted(Comparator.comparing(DBConstraint::getConstraintType).reversed())
                        //.collect(Collectors.toList());
                for(DBConstraint constraint : restoreTable.getConstraints().values()) {
                    String ddl = "";
                    if(constraint.getConstraintType().equals("f")) {//FK
                        ddl += "ALTER TABLE " + schema + ".`" + tableName + "` ADD CONSTRAINT `" + constraint.getName() + "` ";
                        ddl += constraint.getSql().substring(constraint.getSql().toUpperCase().indexOf("FOREIGN KEY"));
                        ddl = ddl.toLowerCase().replace(" not valid", "");
                        ddl = ddl.replace("\"", "`")
                                .replace("\\`", "\\\"");
                        st.execute(ddl);
                    } else {//PK
                        boolean hasNoPK = restoreTable.getFields().values().stream().filter(f -> f.getIsPrimaryKey()).count() == 0;
                        if(hasNoPK) {
                            ddl += "ALTER TABLE " + schema + ".`" + tableName + "` ADD CONSTRAINT `PRIMARY` "
                                  + constraint.getSql().substring(constraint.getSql().toUpperCase().indexOf("PRIMARY KEY"));
                            ddl = ddl.replace("\"", "`")
                                    .replace("\\`", "\\\"");
                            st.execute(ddl);
                        }
                    }
                }
            } else {
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
            }
        } catch (Exception e) {
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                obj.getName()
                ,  "table", obj.getType().getValue()
            ));
        } finally {
            ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
            st.close();
        }
    }
}

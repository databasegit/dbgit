package ru.fusionsoft.dbgit.mysql;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.data_table.RowData;
import ru.fusionsoft.dbgit.data_table.TreeMapRowData;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.stream.Collectors;

public class DBRestoreTableDataMySql extends DBRestoreAdapter {

    @Override
    public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
        if (obj instanceof MetaTableData) {
            MetaTableData currentTableData;
            MetaTableData restoreTableData = (MetaTableData)obj;
            GitMetaDataManager gitMetaMng = GitMetaDataManager.getInstance();
            ////TODO не факт что в кеше есть мета описание нашей таблицы, точнее ее не будет если при старте ресторе таблицы в бд не было совсем
            IMetaObject currentMetaObj = gitMetaMng.getCacheDBMetaObject(obj.getName());
            if (currentMetaObj instanceof MetaTableData || currentMetaObj == null) {
                if(step == 0) {
                    //removeTableConstraintsMySql(restoreTableData.getMetaTable());
                    return false;
                }
                if(step == 1) {
                    String schema = getPhisicalSchema(restoreTableData.getTable().getSchema());
                    schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
                    if (currentMetaObj != null) {
                        currentTableData = (MetaTableData) currentMetaObj;
                    } else {
                        currentTableData = new MetaTableData();
                        currentTableData.setTable(restoreTableData.getTable());
                        currentTableData.getTable().setSchema(schema);
                        currentTableData.setMapRows(new TreeMapRowData());
                        currentTableData.setDataTable(restoreTableData.getDataTable());
                    }
                    currentTableData.getmapRows().clear();
                    if (getAdapter().getTable(schema, currentTableData.getTable().getName()) != null) {
                        currentTableData.setDataTable(getAdapter().getTableData(schema, currentTableData.getTable().getName()));
                        ResultSet rs = currentTableData.getDataTable().getResultSet();
                        TreeMapRowData mapRows = new TreeMapRowData();
                        MetaTable metaTable = new MetaTable(currentTableData.getTable());
                        metaTable.loadFromDB(currentTableData.getTable());
                        if (rs != null) {
                            while(rs.next()) {
                                RowData rd = new RowData(rs, metaTable);
                                mapRows.put(rd.calcRowKey(metaTable.getIdColumns()), rd);
                            }
                        }
                        currentTableData.setMapRows(mapRows);
                    }
                    restoreTableDataMySql(restoreTableData, currentTableData);
                    return false;
                }
                if(step == -2) {
                    //restoreTableConstraintMySql(restoreTableData.getMetaTable());
                    return false;
                }
                return true;
            } else {
                ////TODO WTF????
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
                //return true;
            }
        } else {
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
        }
    }

    @Override
    public void removeMetaObject(IMetaObject obj) throws Exception {
        // TODO Auto-generated method stub
    }

    public void restoreTableDataMySql(MetaTableData restoreTableData, MetaTableData currentTableData) throws Exception {//FIXME
        IDBAdapter adapter = getAdapter();
        Connection connect = adapter.getConnection();
        StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
        try {
            if (restoreTableData.getmapRows() == null)
                restoreTableData.setMapRows(new TreeMapRowData());
            String fields = "";
            if (restoreTableData.getmapRows().size() > 0)
                fields = restoreTableData.getmapRows().firstEntry().getValue().getData().keySet().stream().map(K -> "`" + K + "`").collect(Collectors.joining(", "));
ConsoleWriter.printlnRed("---FIELDS:" + fields);
                //fields = keysToString(restoreTableData.getmapRows().firstEntry().getValue().getData().keySet().stream().map(DBAdapterPostgres::escapeNameIfNeeded).collect(Collectors.toSet())) + " values ";
            MapDifference<String, RowData> diffTableData = Maps.difference(restoreTableData.getmapRows(),currentTableData.getmapRows());
            String schema = getPhisicalSchema(restoreTableData.getTable().getSchema());
            schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
            String tblName = schema + ".`" + restoreTableData.getTable().getName() + "`";
            ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "tableData").withParams(tblName) + "\n", 1);
            ResultSet rsTypes = st.executeQuery("select column_name, data_type from information_schema.columns "
                    + "where table_schema = " + schema + " and table_name = '" + restoreTableData.getTable().getName() + "'");
            HashMap<String, String> colTypes = new HashMap<String, String>();
            while (rsTypes.next()) {
                colTypes.put(rsTypes.getString("column_name"), rsTypes.getString("data_type"));
            }
            //if(!diffTableData.entriesOnlyOnLeft().isEmpty()) {
                //ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "inserting"), 2);
                //for(RowData rowData:diffTableData.entriesOnlyOnLeft().values()) {
                    //ArrayList<String> fieldsList = new ArrayList<String>(rowData.getData().keySet().stream().map(DBAdapterPostgres::escapeNameIfNeeded).collect(Collectors.toList()));
                    //String insertQuery = "insert into " + tblNameEscaped +
                            //fields+valuesToString(rowData.getData().values(), colTypes, fieldsList) + ";\n";
                    //ConsoleWriter.detailsPrintLn(insertQuery);
                    //PrepareStatementLogging ps = new PrepareStatementLogging(connect, insertQuery, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
                    //int i = 0;
                    //for (ICellData data : rowData.getData().values()) {
                        //i++;
                        //ConsoleWriter.detailsPrintLn(data.getSQLData());
                        //ResultSet rs = st.executeQuery("select data_type from information_schema.columns \r\n" +
                                //"where lower(table_schema||'.'||table_name) = lower('" + tblNameUnescaped + "') and lower(column_name) = '" + fieldsList.get(i - 1) + "'");
                        //boolean isBoolean = false;
                        //while (rs.next()) {
                            //if (rs.getString("data_type").contains("boolean")) {
                                //isBoolean = true;
                            //}
                        //}
                        ////ps = setValues(data, i, ps, isBoolean);
                    //}
                    ////if (adapter.isExecSql())
                    ////	ps.execute();
                    ////ps.close();
                    //st.execute(insertQuery);
                //}
                //ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
            //}
            //if(!diffTableData.entriesOnlyOnRight().isEmpty()){
                //ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "deleting"), 2);
                //String deleteQuery="";
                //Map<String,String> primarykeys = new HashMap();
                //for(RowData rowData:diffTableData.entriesOnlyOnRight().values()) {
                    //Map<String, ICellData> tempcols = rowData.getData();
                    //String[] keysArray = rowData.getKey().split("_");
                    //for(String key:keysArray) {
                        //for (String o : tempcols.keySet()) {
                            //if (tempcols.get(o) == null || tempcols.get(o).convertToString() == null) continue;
                            //if (tempcols.get(o).convertToString().equals(key)) {
                                //primarykeys.put(o, tempcols.get(o).convertToString());
                                //tempcols.remove(o);
                                //break;
                            //}
                        //}
                    //}
                    //String delFields="(";
                    //String delValues="(";
                    //StringJoiner fieldJoiner = new StringJoiner(",");
                    //StringJoiner valuejoiner = new StringJoiner(",");
                    //for (Map.Entry<String, String> entry : primarykeys.entrySet()) {
                        //fieldJoiner.add("\""+entry.getKey()+"\"");
                        //valuejoiner.add("\'"+entry.getValue()+"\'");
                    //}
                    //delFields+=fieldJoiner.toString()+")";
                    //delValues+=valuejoiner.toString()+")";
                    //primarykeys.clear();
                    //if (delValues.length() > 3)
                        //deleteQuery+="delete from " + tblNameUnescaped+
                                //" where " + delFields + " = " + delValues + ";\n";
                    //if(deleteQuery.length() > 50000 ){
                        //st.execute(deleteQuery);
                        //deleteQuery = "";
                    //}
                //}
                //if(deleteQuery.length()>1) {
                    //st.execute(deleteQuery);
                //}
                //ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
            //}
            //if(!diffTableData.entriesDiffering().isEmpty()) {
                //ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "updating"), 2);
                //String updateQuery="";
                //Map<String,String> primarykeys = new HashMap();
                //for(ValueDifference<RowData> diffRowData:diffTableData.entriesDiffering().values()) {
                    //if(!diffRowData.leftValue().getHashRow().equals(diffRowData.rightValue().getHashRow())) {
                        //Map<String, ICellData> tempCols = diffRowData.leftValue().getData();
                        //String[] keysArray = diffRowData.leftValue().getKey().split("_");
                        //for(String key:keysArray) {
                            //for (String o : tempCols.keySet()) {
                                //if (tempCols.get(o) == null || tempCols.get(o).convertToString() == null) continue;
                                //if (tempCols.get(o).convertToString().equals(key)) {
                                    //primarykeys.put(o, tempCols.get(o).convertToString());
                                    //tempCols.remove(o);
                                    //break;
                                //}
                            //}
                        //}
                        //if(!tempCols.isEmpty()) {
                            //String keyFields="(";
                            //String keyValues="(";
                            //StringJoiner fieldJoiner = new StringJoiner(",");
                            //StringJoiner valuejoiner = new StringJoiner(",");
                            //for (Map.Entry<String, String> entry : primarykeys.entrySet()) {
                                //fieldJoiner.add("\""+entry.getKey()+"\"");
                                //valuejoiner.add("\'"+entry.getValue()+"\'");
                            //}
                            //keyFields+=fieldJoiner.toString()+")";
                            //keyValues+=valuejoiner.toString()+")";
                            //primarykeys.clear();
                            //StringJoiner updfieldJoiner = new StringJoiner(",");
                            //StringJoiner updvaluejoiner = new StringJoiner(",");
                            //String updFields="(";
                            //String updValues="(";
                            //for (Map.Entry<String, ICellData> entry : tempCols.entrySet()) {
                                //updfieldJoiner.add("\""+entry.getKey()+"\"");
                                ////updvaluejoiner.add("\'"+entry.getValue().convertToString()+"\'");
                                ////updvaluejoiner.add("?");
                            //}
                            //ArrayList<String> fieldsList = new ArrayList<String>(diffRowData.leftValue().getData().keySet());
                            //updFields+=updfieldJoiner.toString()+")";
                            //updValues+=updvaluejoiner.toString()+")";
                            //updateQuery="update "+tblNameEscaped+
                                    //" set "+updFields + " = " + valuesToString(tempCols.values(), colTypes, fieldsList) + " where " + keyFields+ "=" +keyValues+";\n";
                            //ConsoleWriter.detailsPrintLn(updateQuery);
                            //PrepareStatementLogging ps = new PrepareStatementLogging(connect, updateQuery, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
                            //int i = 0;
                            //ConsoleWriter.detailsPrintLn("vals for " + keyValues + ":" + diffRowData.leftValue().getData().values());
                            //for (ICellData data : diffRowData.leftValue().getData().values()) {
                                //i++;
                                //ConsoleWriter.detailsPrintLn(data.getSQLData());
                                //ResultSet rs = st.executeQuery("select data_type from information_schema.columns \r\n" +
                                        //"where lower(table_schema||'.'||table_name) = lower('" + tblNameUnescaped + "') and lower(column_name) = '" + fieldsList.get(i - 1) + "'");
                                //boolean isBoolean = false;
                                //while (rs.next()) {
                                    //if (rs.getString("data_type").toLowerCase().contains("boolean")) {
                                        //isBoolean = true;
                                    //}
                                //}
                                ////ps = setValues(data, i, ps, isBoolean);
                            //}
							///*
							//if (adapter.isExecSql())
								//ps.execute();
							//ps.close();
							//updateQuery = "";
							//*/
                            ////if(updateQuery.length() > 50000 ){
                            //st.execute(updateQuery);
                            //updateQuery = "";
                            ////}
                        //}
                    //}
                //}
                //ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
                //if(updateQuery.length()>1) {
                    //ConsoleWriter.println(updateQuery);
                    //st.execute(updateQuery);
                //}
            //}
        } catch (Exception e) {
            ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
            throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(restoreTableData.getTable().getSchema() + "." + restoreTableData.getTable().getName()), e);
        } finally {
            st.close();
        }
    }
}

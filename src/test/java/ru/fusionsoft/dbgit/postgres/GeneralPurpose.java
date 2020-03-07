package ru.fusionsoft.dbgit.postgres;


import com.google.common.collect.Lists;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.oracle.DBRestorePackageOracle;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import javax.naming.Name;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

import static org.junit.Assert.*;


public class GeneralPurpose {

    public static Properties testProps;


    public static String TEST_CONN_URL = "localhost";
    public static String TEST_CONN_CATALOG = "Test";
    public static String TEST_CONN_STRING = "jdbc:postgresql://"+TEST_CONN_URL+";databaseName="+TEST_CONN_CATALOG+";integratedSecurity=false;";
    public static String TEST_CONN_USER = "postgres";
    public static String TEST_CONN_PASS = "Kan:al*098";

    private static DBAdapterPostgres testAdapter;
    private static DBBackupAdapterPostgres testBackup;
    private static FactoryDBAdapterRestorePostgres testRestoreFactory = new FactoryDBAdapterRestorePostgres();
    private static DBRestoreTriggerPostgres restoreTrigger;


    private static Connection testConnection;
    private static boolean isInitialized = false;
    private static boolean isMasterDatabase;

    static{
        testProps = new Properties();
        testProps.setProperty("url", TEST_CONN_STRING);
        testProps.setProperty("user", TEST_CONN_USER);
        testProps.setProperty("password", TEST_CONN_PASS);
        testProps.put("characterEncoding", "UTF-8");
    }


    private static String tableName = "TestTableSome";
    private static String triggerName = "TestTrigger";
    private static String triggerNameEncr = "TestTriggerEncrypted";
    private static String procedureName = "TestProc";
    private static String functionName = "TestFunc";
    private static String functionNameTable = functionName + "Table";
    private static String triggerTableName = "TestTableTrigger";
    private static String schema;
    private static String viewName = "TestView";
    private static String sequenceName = "TestSequence";


    @Before
    public void setUp() throws Exception {
        if(!isInitialized){
            try {
                String url = testProps.getProperty("url");
                testConnection = DriverManager.getConnection(url, testProps);
                testConnection.setAutoCommit(false);
                testAdapter = (DBAdapterPostgres) AdapterFactory.createAdapter(testConnection);
                testBackup = (DBBackupAdapterPostgres) testAdapter.getBackupAdapterFactory().getBackupAdapter(testAdapter);
                restoreTrigger = (DBRestoreTriggerPostgres) testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitTrigger,testAdapter);
                isMasterDatabase = testConnection.getCatalog().equalsIgnoreCase("master");
                schema = testConnection.getSchema();
                isInitialized = true;
            }
            catch (Exception ex){
                fail(ex.getMessage());
            }
            dropBackupTables();
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void Dosome() {
        
    }


    //heplers

    public void dropBackupTables() throws Exception{
        Map<String, DBTable> tables = testAdapter.getTables(schema);
        for (DBTable table : tables.values()){
            if(table.getName().startsWith("BACKUP$")){
                dropTable(schema+"."+table.getName());
            }
        }
    }

    public void executeSqlInMaster(String expression, boolean commitAfter) throws SQLException{
        testConnection.setCatalog("master");

        Statement stmt = testConnection.createStatement();
        stmt.execute(expression);
        stmt.close();

        if(commitAfter) testConnection.commit();
        testConnection.setCatalog(TEST_CONN_CATALOG);
    }

    public void executeSqlInMaster(List<String> expressions, boolean commitAfter) throws SQLException{
        testConnection.setCatalog("master");

        Statement stmt = testConnection.createStatement();
        for(String expr : expressions)  stmt.execute(expr);
        stmt.close();

        if(commitAfter) testConnection.commit();
        testConnection.setCatalog(TEST_CONN_CATALOG);

    }

    public void dropSchema(String schemaName) throws Exception{
        Statement stmt = testConnection.createStatement();
        stmt.execute(
                "IF EXISTS ( SELECT  * FROM sys.schemas WHERE name = N'"+schemaName+"' )\n" +
                        "DROP SCHEMA ["+schemaName+"];"
        );
        stmt.close();
    }

    public void dropRole(String roleName) throws Exception{
        Statement stmt = testConnection.createStatement();
        stmt.execute(
                "IF EXISTS (SELECT 1 FROM sys.database_principals WHERE name='"+roleName+"' AND Type = 'R')" +
                        "DROP ROLE ["+roleName+"];"
        );
        stmt.close();
    }

    public DBAdapterPostgres createAdapterWithCredentials(String username, String password, String url) throws Exception{
        Properties props = new Properties();
        props.setProperty("url", Objects.nonNull(url) ? url : TEST_CONN_STRING);
        props.setProperty("user", Objects.nonNull(username) ? username : TEST_CONN_USER);
        props.setProperty("password", Objects.nonNull(password) ? password : TEST_CONN_USER);
        props.put("characterEncoding", "UTF-8");

        Connection conn = DriverManager.getConnection(props.getProperty("url"), props);
        conn.setAutoCommit(false);

        DBAdapterPostgres adapter = new DBAdapterPostgres();
        adapter.setConnection(conn);
        adapter.registryMappingTypes();

        return adapter;
    }

    public void createUserAndLogin(String userName, String loginName, String schemaName, String password, boolean isHashed) throws Exception{
        Statement stmt = testConnection.createStatement();
        String passExpr = (isHashed) ? password + " HASHED" : "'" + password + "'";

        List<String> createUserExprs = Arrays.asList(
                (
                        "IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'"+userName+"') DROP USER ["+userName+"];" +
                                "IF EXISTS (SELECT * FROM sys.server_principals WHERE name = N'"+loginName+"') DROP LOGIN ["+loginName+"];" +
                                "CREATE LOGIN ["+loginName+"] WITH PASSWORD = "+passExpr+";" +
                                "CREATE USER ["+userName+"] FOR LOGIN ["+loginName+"] WITH DEFAULT_SCHEMA = ["+schemaName+"];"
                ).split(";")
        );

        for(String expr : createUserExprs) stmt.execute(expr);
        stmt.close();
        testConnection.commit();

        executeSqlInMaster("GRANT CONNECT SQL TO ["+loginName+"]", true);

    }

    public void dropUserAndLogin(String userName, String loginName) throws Exception{
        String dropUserExpr = "DROP LOGIN ["+loginName+"]; DROP USER ["+userName+"]";
        Statement stmt = testConnection.createStatement();
        stmt.execute(dropUserExpr);
        stmt.close();
    }

    public void addToRole(String userName, String roleName) throws Exception{
        Statement stmt = testConnection.createStatement();
        stmt.execute("EXECUTE sp_AddRoleMember '"+roleName+"', '"+userName+"';");
        stmt.close();
    }

    public String createTable(String schemaAndName, String fieldsExpr) throws Exception{

        try (Statement stmt = testConnection.createStatement()){
            String name = convertSchemaAndName(schemaAndName);
            String ddl = "CREATE TABLE "+schemaAndName+"("+fieldsExpr+") ON [PRIMARY]\n";
            stmt.execute("IF OBJECT_ID('"+name+"', 'U') IS NOT NULL DROP TABLE " + name);
            stmt.execute(ddl);
            return ddl;
        }
    }

    public void dropTable(String schemaName, String tableName) throws Exception{
        dropTable(schemaName+"."+tableName);
    }

    public void dropTable(String schemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();
        String name = convertSchemaAndName(schemaAndName);
        String ddl = MessageFormat.format("IF OBJECT_ID(''{0}'', ''U'') IS NOT NULL DROP TABLE {0}", name);
        stmt.execute(ddl);
        stmt.close();
    }

    public String convertSchemaAndName(String san) {
        return san.startsWith("#")
                ? "tempdb.." + san.substring(1)
                : san;
    }

    //entire sets

    public AutoCloseable useTestView(String schemaName, String viewName) throws Exception{
        return new AutoCloseable() {
            private Statement stmt = testConnection.createStatement();

            {
                stmt.execute("IF OBJECT_ID('"+schemaName+" ."+viewName+"', 'V') IS NOT NULL DROP VIEW "+schemaName+"."+viewName+" \n");
                stmt.execute(
                        "CREATE VIEW "+schemaName+"."+viewName+" AS\n" +
                                "SELECT SC.name + '(' + SO.NAME + ')' as theUsualTitle\n" +
                                "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n"
                );
                stmt.execute(
                        "ALTER VIEW "+schemaName+"."+viewName+" AS\n" +
                                "SELECT 'THE ALLMIGHT ''' + SC.name + ''' FROM ' + SO.name  as theBigTitle\n" +
                                "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n"
                );
            }

            @Override
            public void close() throws Exception {
                //did you know that in MessageFormat.format (') single quotes should be escaped by another ' single quote -> ('') ?
                stmt.execute(MessageFormat.format("IF OBJECT_ID(''{0}.{1}'', ''V'') IS NOT NULL DROP VIEW {0}.{1} \n", schemaName, viewName));
                stmt.close();
            }
        };
    }

    private String createTestView(String schemaName, String viewName) throws Exception{
        String viewDDl =
                "CREATE VIEW "+schemaName+"."+viewName+" AS\n" +
                        "SELECT 'THE ALLMIGHT ''' + SC.name + ''' FROM ' + SO.name  as theBigTitle\n" +
                        "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id";

        Statement stmt = testConnection.createStatement();
        stmt.execute("IF OBJECT_ID('"+schemaName+" ."+viewName+"', 'V') IS NOT NULL DROP VIEW "+schemaName+"."+viewName+" \n");
        stmt.execute(
                "CREATE VIEW "+schemaName+"."+viewName+" AS\n" +
                        "SELECT SC.name + '(' + SO.NAME + ')' as theUsualTitle\n" +
                        "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n");
        stmt.execute(
                "ALTER VIEW "+schemaName+"."+viewName+" AS\n" +
                        "SELECT 'THE ALLMIGHT ''' + SC.name + ''' FROM ' + SO.name  as theBigTitle\n" +
                        "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n");
        stmt.close();

        return viewDDl;

    }

    public void dropTestView(String schemaName, String viewName) throws Exception{
        Statement stmt = testConnection.createStatement();

        stmt.execute("IF OBJECT_ID('"+schemaName+"."+viewName+"', 'V') IS NOT NULL DROP VIEW "+schemaName+"."+viewName+" \n");
        stmt.close();
        testConnection.commit();
    }

}
package ru.fusionsoft.dbgit.postgres;


import com.google.common.collect.Lists;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.oracle.DBRestorePackageOracle;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import javax.naming.Name;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;


public class DependencyAwareTest {

    public static Properties testProps;


    public static String TEST_CONN_URL = "localhost";
    public static String TEST_CONN_CATALOG = "Test";
    public static String TEST_CONN_STRING = "jdbc:postgresql://"+TEST_CONN_URL + "/" + TEST_CONN_CATALOG;
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


    private static String tableName = "SomeTable";
    private static String functionName = "TestFunc";
    private static String schema = "public";
    private static String otherSchema = "other";
    private static String viewNameL1 = "TestViewL1";
    private static String viewNameL2 = "TestViewL2";
    private static String sequenceName = "SomeSequence";


    @Before
    public void setUp() {
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
            //dropBackupTables();
        }
    }

    @Test
    public void getIMetaObjectOrder() throws Exception{
        createTestObjects();

        IMetaObject mv = new MetaView();
        int order = DBAdapter.getIMetaObjectOrder(mv);
        assertEquals(DBAdapter.imoOrders.indexOf(MetaView.class), order);
    }

    @Test
    public void getViewPostgres(){
        testAdapter.getViews(schema);
    }

    @Test
    public void getViewsPostgres(){
        testAdapter.getView(schema, "dependency");
    }

    @Test
    public void sort() throws Exception {
        createTestObjects();
        MetaTable mt = new MetaTable(testAdapter.getTable(schema, tableName));
        MetaFunction mf = new MetaFunction(testAdapter.getFunction(schema, functionName));
        MetaView mv1 = new MetaView(testAdapter.getView(otherSchema, viewNameL1));
        MetaView mv2 = new MetaView(testAdapter.getView(schema, viewNameL2));

        List<IMetaObject> metaObjects = new ArrayList<>();

        metaObjects.add(mv2);
        metaObjects.add(mv1);
        metaObjects.add(mt);
        metaObjects.add(mf);

        metaObjects.sort(testAdapter.imoDependenceComparator);

        assertTrue(metaObjects.indexOf(mt) < metaObjects.indexOf(mf));
        assertTrue(metaObjects.indexOf(mv1) < metaObjects.indexOf(mv2));
        assertTrue(metaObjects.indexOf(mt) < metaObjects.indexOf(mv1));
    }

    @Test
    public void createTestObjects() throws SQLException {
        executeSqlInMaster(Arrays.asList(
                "CREATE TABLE IF NOT EXISTS "+schema+".\""+tableName+"\"\n" +
                "(\n" +
                "    \"someKey\" integer NOT NULL,\n" +
                "    \"someValue\" text,\n" +
                "    PRIMARY KEY (\"someKey\")\n" +
                ")",

                "create schema if not exists "+otherSchema,

                "CREATE OR REPLACE VIEW "+otherSchema+".\""+viewNameL1+"\"\n" +
                " AS SELECT \"someKey\",\"someValue\"\n" +
                " FROM public.\"SomeTable\";",

                "CREATE OR REPLACE FUNCTION "+schema+".\""+functionName+"\"(val text) RETURNS text AS $$\n" +
                "BEGIN\n" +
                "RETURN val || 'Some';\n" +
                "END; \n" +
                "$$ LANGUAGE PLPGSQL;",

                "CREATE SEQUENCE IF NOT EXISTS "+schema+".\""+sequenceName+"\"\n" +
                "    INCREMENT 1\n" +
                "    START 0\n" +
                "    MINVALUE 0;",

                "CREATE OR REPLACE VIEW "+schema+".\""+viewNameL2+"\" AS \n" +
                "SELECT DISTINCT "+schema+".\""+functionName+"\"(v.\"someValue\") as transformedValue\n" +
                "FROM "+otherSchema+".\""+viewNameL1+"\" v"
            ),
            true
        );


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

    public String convertSchemaAndName(String san) {
        return san.startsWith("#")
                ? "tempdb.." + san.substring(1)
                : san;
    }

}
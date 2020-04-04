package ru.fusionsoft.dbgit.postgres;


import org.junit.Before;
import org.junit.Test;
import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.dbobjects.IDBObject;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;

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
    private static String publicSchema = "public";
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

                isInitialized = true;
            }
            catch (Exception ex){
                fail(ex.getMessage());
            }
            //dropBackupTables();
        }
    }

    @Test
    public void getTablesPostgres() {
        Map<String, DBTable> tables = testAdapter.getTables(publicSchema);
        tables.values().forEach( x -> {
            DBTable table = testAdapter.getTable(x.getSchema(), x.getName());
            ConsoleWriter.println("table " + table.getSchema() + "." + table.getName() +", its deps:");
            x.getDependencies().forEach(ConsoleWriter::println);
        });
    }

    @Test
    public void getViewPostgres(){
        testAdapter.getViews(publicSchema);
    }

    @Test
    public void getViewsPostgres(){
        testAdapter.getView(publicSchema, "dependency");
    }

    @Test
    public void getIndexesPostgres() throws Exception {
        testAdapter.getIndexes("loader", "t$predecessors");
    }

    @Test
    public void sort() throws Exception {
        createTestObjects();
        MetaTable mt = new MetaTable(testAdapter.getTable(publicSchema, tableName));
        MetaFunction mf = new MetaFunction(testAdapter.getFunction(publicSchema, functionName));
        MetaView mv1 = new MetaView(testAdapter.getView(otherSchema, viewNameL1));
        MetaView mv2 = new MetaView(testAdapter.getView(publicSchema, viewNameL2));

        List<IMetaObject> metaObjects = new ArrayList<>();

        metaObjects.add(mv2);
        metaObjects.add(mv1);
        metaObjects.add(mt);
        metaObjects.add(mf);

        metaObjects.sort(DBAdapter.imoDependenceComparator);

        assertTrue(metaObjects.indexOf(mt) < metaObjects.indexOf(mf));
        assertTrue(metaObjects.indexOf(mv1) < metaObjects.indexOf(mv2));
        assertTrue(metaObjects.indexOf(mt) < metaObjects.indexOf(mv1));
    }

    @Test
    public void restoreDataBase() throws Exception{
        createTestObjects();

        List<IDBObject> toDeleteDBOs = new ArrayList<>();
        toDeleteDBOs.addAll( testAdapter.getViews(publicSchema).values());
        toDeleteDBOs.addAll( testAdapter.getViews(otherSchema).values());

        IMapMetaObject toDeleteMOs = new TreeMapMetaObject();
        for (IDBObject dbo : toDeleteDBOs){
            toDeleteMOs.put(new MetaView((DBView) dbo));
        }

        toDeleteDBOs.clear();
        toDeleteDBOs.add( testAdapter.getTable(publicSchema, tableName) );
        for (IDBObject dbo : toDeleteDBOs){
            toDeleteMOs.put(new MetaTable((DBTable) dbo));
        }

        dropBackupObjects();
        testAdapter.deleteDataBase(toDeleteMOs);
        testAdapter.restoreDataBase(toDeleteMOs);
    }

    public void createTestObjects() throws SQLException {
        executeSqlInMaster(Arrays.asList(
                "CREATE SCHEMA IF NOT EXISTS "+publicSchema,

                "CREATE TABLE IF NOT EXISTS "+ publicSchema +".\""+tableName+"\"\n" +
                "(\n" +
                "    \"someKey\" integer NOT NULL,\n" +
                "    \"someValue\" text,\n" +
                "    PRIMARY KEY (\"someKey\")\n" +
                ")",

                "CREATE SCHEMA IF NOT EXISTS "+otherSchema,

                "CREATE OR REPLACE VIEW "+otherSchema+".\""+viewNameL1+"\"\n" +
                " AS SELECT \"someKey\",\"someValue\"\n" +
                " FROM public.\"SomeTable\";",

                "CREATE OR REPLACE FUNCTION "+ publicSchema +".\""+functionName+"\"(val text) RETURNS text AS $$\n" +
                "BEGIN\n" +
                "RETURN val || 'Some';\n" +
                "END; \n" +
                "$$ LANGUAGE PLPGSQL;",

                "CREATE SEQUENCE IF NOT EXISTS "+ publicSchema +".\""+sequenceName+"\"\n" +
                "    INCREMENT 1\n" +
                "    START 0\n" +
                "    MINVALUE 0;",

                "CREATE OR REPLACE VIEW "+ publicSchema +".\""+viewNameL2+"\" AS \n" +
                "SELECT DISTINCT "+ publicSchema +".\""+functionName+"\"(v.\"someValue\") as transformedValue\n" +
                "FROM "+otherSchema+".\""+viewNameL1+"\" v"
            ),
            true
        );


    }

    public void dropBackupObjects() throws SQLException {

        String prefix = "BACKUP$";
        try( Statement st = testConnection.createStatement() ) {
            StringBuilder sb = new StringBuilder();
            for(String schema : Arrays.asList(publicSchema, otherSchema)){
                testAdapter.getViews(schema).values().stream().filter( x->x.getName().contains(prefix ) ).sorted(DBAdapter.dbsqlComparator).forEach(x-> {
                        sb.append( MessageFormat.format("DROP VIEW {0}.{1};\n", x.getSchema(), DBAdapterPostgres.escapeNameIfNeeded(x.getName())));
                });
               testAdapter.getTables(schema).values().stream().filter( x->x.getName().contains(prefix ) ).forEach(x-> {
                        sb.append( MessageFormat.format("DROP TABLE {0}.{1};\n", x.getSchema(), DBAdapterPostgres.escapeNameIfNeeded(x.getName())));
                });
               testAdapter.getFunctions(schema).values().stream().filter( x->x.getName().contains(prefix ) ).sorted(DBAdapter.dbsqlComparator).forEach(x-> {
                        sb.append( MessageFormat.format("DROP FUNCTION {0}.{1};\n", x.getSchema(), DBAdapterPostgres.escapeNameIfNeeded(x.getName())));
                });
               /*
               testAdapter.getProcedures(schema).values().stream().filter( x->x.getName().contains(prefix ) ).sorted(DBAdapter.dbsqlComparator).forEach(x-> {
                        sb.append( MessageFormat.format("DROP PROCEDURE {0}.{1};\n", x.getSchema(), DBAdapterPostgres.escapeNameIfNeeded(x.getName())));
                });
               */
               testAdapter.getTriggers(schema).values().stream().filter( x->x.getName().contains(prefix ) ).sorted(DBAdapter.dbsqlComparator).forEach(x-> {
                        sb.append( MessageFormat.format("DROP TRIGGER {0}.{1};\n", x.getSchema(), DBAdapterPostgres.escapeNameIfNeeded(x.getName())));
                });
            }
           st.execute(sb.toString());
           testConnection.commit();

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
        testConnection.setCatalog(TEST_CONN_CATALOG);

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
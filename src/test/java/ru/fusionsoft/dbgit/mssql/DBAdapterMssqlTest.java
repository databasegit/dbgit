package ru.fusionsoft.dbgit.mssql;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.*;


public class DBAdapterMssqlTest {

    public static Properties testProps;

    /*
    *  Using SQL EXPRESS 2012\Win 10 Home forced me to:
    *  - via CLICONFG.EXE -> Aliases: move TCP/IP protocol from disabled to enabled list
    *  - via CLICONFG.EXE -> Common : create alias  with localhost -> localhost:1433 (port 1433 in connString is fake)
    *
    *  Setup of ip an ports via SQLServerManager11.msc also needed, mine was:
    *  Protocols -> SQL EXPRESS protocols -> Protocol -> Listen all to true and
    *  Protocols -> SQL EXPRESS protocols -> Protocol -> IP adresses -> IPAll -> TCP Port to 1433 and Dynamic TCP Port to blank
    * */
    public static String TEST_CONN_STRING = "jdbc:sqlserver://localhost:1433;databaseName=master;integratedSecurity=false;";
    public static String TEST_CONN_USER = "test";
    public static String TEST_CONN_PASS = "test";

    private DBAdapterMssql testAdapter;
    private Connection testConnection;

    static{
        testProps = new Properties();
        testProps.setProperty("url", TEST_CONN_STRING);
        testProps.setProperty("user", TEST_CONN_USER);
        testProps.setProperty("password", TEST_CONN_PASS);
        testProps.put("characterEncoding", "UTF-8");
    }

    @Before
    public void setUp() throws Exception {
        String url = testProps.getProperty("url");
        testProps.remove("url");
        testConnection = DriverManager.getConnection(url, testProps);
        testConnection.setAutoCommit(false);
        testAdapter = (DBAdapterMssql)  AdapterFactory.createAdapter(testConnection);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getSchemes() {
        Map<String, DBSchema> schemes = testAdapter.getSchemes();
        assertTrue(schemes.containsKey("guest"));
        assertTrue(schemes.containsKey("dbo"));
    }

    @Test
    public void getTableSpaces() {
        Map<String, DBTableSpace> tablespaces = testAdapter.getTableSpaces();
        assertEquals("ROWS_FILEGROUP", tablespaces.get("PRIMARY").getOptions().getChildren().get("type_desc").getData());
    }

    @Test
    public void getSequences() {
        try{
            testConnection.createStatement().execute(
                "IF EXISTS (SELECT * FROM sys.sequences WHERE NAME = N'TEST_SEQUENCE' AND TYPE='SO')\n" +
                "DROP Sequence TEST_SEQUENCE\n" +
                "CREATE SEQUENCE TEST_SEQUENCE\n" +
                "START WITH 1\n" +
                "INCREMENT BY 1;\n"
            );

            Map<String, DBSequence> sequences = testAdapter.getSequences("dbo");
            assertEquals("dbo", sequences.get("TEST_SEQUENCE").getOptions().get("owner").getData());
            testConnection.createStatement().execute("DROP Sequence TEST_SEQUENCE\n");
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getSequence() {
        String name = "TEST_SEQUENCE";
        try{
            testConnection.createStatement().execute(
                    "IF EXISTS (SELECT * FROM sys.sequences WHERE NAME = N'" + name + "' AND TYPE='SO')\n" +
                            "DROP Sequence " + name + "\n" +
                            "CREATE Sequence " + name + "\n" +
                            "START WITH 1\n" +
                            "INCREMENT BY 1;\n"
            );

            DBSequence sequence = testAdapter.getSequence("dbo", name);
            assertEquals(name, sequence.getOptions().get("name").getData());
            testConnection.createStatement().execute("DROP Sequence " + name + "\n");
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }
}
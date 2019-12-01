package ru.fusionsoft.dbgit.mssql;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.dbobjects.*;

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
    private static DBAdapterMssql testAdapter;
    private static Connection testConnection;
    private static boolean isInitialized = false;

    static{
        testProps = new Properties();
        testProps.setProperty("url", TEST_CONN_STRING);
        testProps.setProperty("user", TEST_CONN_USER);
        testProps.setProperty("password", TEST_CONN_PASS);
        testProps.put("characterEncoding", "UTF-8");
    }


    @Before
    public void setUp() throws Exception {
        if(isInitialized) return;
        try {
            String url = testProps.getProperty("url");
            testProps.remove("url");
            testConnection = DriverManager.getConnection(url, testProps);
            testConnection.setAutoCommit(false);
            testAdapter = (DBAdapterMssql) AdapterFactory.createAdapter(testConnection);
            isInitialized = true;
        }
        catch (Exception ex){
            fail(ex.getMessage());
        }
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

    @Test
    public void getTables() {
        String name = "TEST_TABLE";
        try{
            testConnection.createStatement().execute(
                    "IF OBJECT_ID('dbo.Scores', 'U') IS NOT NULL \n" +
                        "DROP TABLE dbo." + name + "\n" +
                        "CREATE TABLE " + name + "\n(" +
                            "PersonID int,\n" +
                            "LastName varchar(255),\n" +
                            "FirstName varchar(255),\n" +
                            "Address varchar(255),\n" +
                            "City varchar(255)\n" +
                            "); "
            );

            Map<String, DBTable> tables = testAdapter.getTables("dbo");
            assertEquals(name, tables.get("TEST_TABLE").getOptions().get("name").getData());
            testConnection.createStatement().execute("DROP TABLE dbo." + name + "\n" );
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTableFields() {
        String name = "TestTableTypes";
        try{
            testConnection.createStatement().execute(
                    "IF OBJECT_ID('dbo."+ name + "', 'U') IS NOT NULL \n" +
                            "DROP TABLE dbo." + name + "\n" +
                            "CREATE TABLE [dbo].[TestTableTypes](\n" +
                            "	[col1] [nchar](10) NULL,\n" +
                            "	[col2] [ntext] NULL,\n" +
                            "	[col3] [numeric](18, 0) NULL,\n" +
                            "	[col4] [nvarchar](50) NULL,\n" +
                            "	[col5] [nvarchar](max) NULL,\n" +
                            "	[col6] [real] NULL,\n" +
                            "	[col7] [smalldatetime] NULL,\n" +
                            "	[col8] [smallint] NULL,\n" +
                            "	[col9] [smallmoney] NULL,\n" +
                            "	[col10] [sql_variant] NULL,\n" +
                            "	[col11] [text] NULL,\n" +
                            "	[col12] [time](7) NULL,\n" +
                            "	[col13] [timestamp] NULL,\n" +
                            "	[col14] [tinyint] NULL,\n" +
                            "	[col15] [uniqueidentifier] NULL,\n" +
                            "	[col16] [varbinary](50) NULL,\n" +
                            "	[col17] [varbinary](max) NULL,\n" +
                            "	[col18] [varchar](50) NULL,\n" +
                            "	[col19] [varchar](max) NULL,\n" +
                            "	[col20] [xml] NULL\n" +
                            ") ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]"
            );

            Map<String, DBTableField> fields = testAdapter.getTableFields("dbo", "TestTableTypes");
            assertEquals("sql_variant(0)", fields.get("col10").getTypeSQL());
            assertEquals("native", fields.get("col10").getTypeUniversal());

            testConnection.createStatement().execute("DROP TABLE dbo." + name + "\n" );
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }
}
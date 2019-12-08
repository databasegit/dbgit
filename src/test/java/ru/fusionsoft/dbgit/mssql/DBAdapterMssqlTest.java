package ru.fusionsoft.dbgit.mssql;

import com.google.common.collect.Lists;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.dbobjects.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

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

    @Test
    public void getIndexes() {
        String indexCreateDdl = "CREATE NONCLUSTERED INDEX [IX_IdTest] ON [dbo].[AspNetRolesTest] ([Id]) ON [PRIMARY];";

        try{
            testConnection.createStatement().execute(
            "CREATE TABLE [dbo].[AspNetRolesTest](\n" +
                "	[Id] [nvarchar](128) NOT NULL,\n" +
                "	[Name] [nvarchar](256) NOT NULL,\n" +
                " CONSTRAINT [PK_dbo.AspNetRolesTest] PRIMARY KEY CLUSTERED \n" +
                "(\n" +
                "	[Id] ASC\n" +
                ")WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n" +
                ") ON [PRIMARY]\n" + indexCreateDdl
            );

            Map<String, DBIndex> indexes = testAdapter.getIndexes("dbo", "AspNetRolesTest");
            assertEquals("2", indexes.get("IX_IdTest").getOptions().getChildren().get("indexid").getData());
            assertEquals(indexCreateDdl, indexes.get("IX_IdTest").getSql());
            testConnection.createStatement().execute("DROP TABLE dbo.AspNetRolesTest" );
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getConstraints() {

        String constrDDL1 = "ALTER TABLE dbo.ConstraintsTestTable ADD CONSTRAINT df_constraint DEFAULT ('{}') FOR [value];";
        String constrDDL2 = "ALTER TABLE dbo.ConstraintsTestTable ADD CONSTRAINT df_constraintInt DEFAULT ((1)) FOR [valueCheck1];";
        String constrDDL3 = "ALTER TABLE dbo.ConstraintsTestTable ADD CONSTRAINT u_constraint UNIQUE NONCLUSTERED ([valueUnique]);";
        String constrDDL4 = "ALTER TABLE dbo.ConstraintsTestTable ADD CONSTRAINT chk_constraint CHECK ([valueCheck1]>(0) AND [valueCheck2]>(0));";
        String constrDDL5 = "ALTER TABLE dbo.ConstraintsTestTable ADD CONSTRAINT fk_constraint FOREIGN KEY (fkInt) references dbo.FKTest(keyInt);";
        try{
            testConnection.createStatement().execute(
            "IF OBJECT_ID('dbo.ConstraintsTestTable', 'U') IS NOT NULL \n" +
                "DROP TABLE dbo.ConstraintsTestTable;\n" +
                "CREATE TABLE dbo.ConstraintsTestTable (\n" +
                "	[key] varchar(20) PRIMARY KEY, \n" +
                "	[value] varchar(20) NOT NULL, \n" +
                "	[valueCheck1] int NOT NULL, \n" +
                "	[valueCheck2] int NOT NULL,\n" +
                "	[valueUnique] varchar(20),\n" +
                "	[fkInt] int\n" +
                ") ON [PRIMARY];\n" +
                "IF OBJECT_ID('dbo.FKTest', 'U') IS NOT NULL DROP TABLE dbo.FKTest;\n" +
                "CREATE TABLE dbo.FKTest( keyInt int PRIMARY KEY, valueChar nvarchar(100) );\n" +
                "SELECT valueCheck1, valueCheck2 from dbo.ConstraintsTestTable; \n" +
                constrDDL1 + constrDDL2 + constrDDL3 + constrDDL4 + constrDDL5
            );


            Map<String, DBConstraint> constraints = testAdapter.getConstraints("dbo", "ConstraintsTestTable");
            assertEquals(constrDDL1, constraints.get("df_constraint").getSql());
            assertEquals(constrDDL2, constraints.get("df_constraintInt").getSql());
            assertEquals(constrDDL3, constraints.get("u_constraint").getSql());
            assertEquals(constrDDL4, constraints.get("chk_constraint").getSql());
            assertEquals(constrDDL5, constraints.get("fk_constraint").getSql());

            testConnection.createStatement().execute("DROP TABLE dbo.ConstraintsTestTable;\n DROP TABLE dbo.FKTest\n" );
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getViews() {

        String viewDDl =
            "CREATE VIEW dbo.testView AS\n" +
            "SELECT 'THE ALLMIGHT ''' + SC.name + ''' FROM ' + SO.name  as theBigTitle\n" +
            "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id";

        try{
            Statement stmt = testConnection.createStatement();
            stmt.execute("IF OBJECT_ID('dbo.testView', 'V') IS NOT NULL DROP VIEW dbo.testView \n");
            stmt.execute(
                "CREATE VIEW dbo.testView AS\n" +
                "SELECT SC.name + '(' + SO.NAME + ')' as theUsualTitle\n" +
                "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n");
            stmt.execute(
                "ALTER VIEW dbo.testView AS\n" +
                "SELECT 'THE ALLMIGHT ''' + SC.name + ''' FROM ' + SO.name  as theBigTitle\n" +
                "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n");

            Map<String, DBView> views = testAdapter.getViews("dbo");
            assertEquals(viewDDl, views.get("testView").getSql());

            stmt.execute("DROP VIEW dbo.testView;" );
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getView() {

        try{
            String viewDDl = createTestView();

            DBView view = testAdapter.getView("dbo", "testView");
            assertEquals(viewDDl, view.getSql());

            testConnection.createStatement().execute("DROP VIEW dbo.testView;" );
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getProcedures() {

        try{
            List<String> ddls = createTestExecutables();

            Map<String, DBProcedure> procedures = testAdapter.getProcedures("dbo");
            assertEquals(ddls.get(6), procedures.get("ProcedureTest").getSql());

            dropTestExecutables();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getProcedure() {

        try{
            List<String> ddls = createTestExecutables();

            DBProcedure procedure = testAdapter.getProcedure("dbo", "ProcedureTest");
            assertEquals(ddls.get(6), procedure.getSql());

            dropTestExecutables();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getFunctions(){

        try{
            List<String> ddls = createTestExecutables();

            Map<String, DBFunction> functions = testAdapter.getFunctions("dbo");
            assertEquals(ddls.get(4), functions.get("FunctionTestTable").getSql());

            dropTestExecutables();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getFunction() {

        try{
            List<String> ddls = createTestExecutables();

            DBFunction function = testAdapter.getFunction("dbo", "FunctionTestScalar");
            assertEquals(ddls.get(2), function.getSql());

            dropTestExecutables();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTriggers() {

        try{
            List<String> ddls = createTestExecutables();

            Map<String, DBTrigger> triggers = testAdapter.getTriggers("dbo");
            assertEquals(ddls.get(7), triggers.get("TriggerTest").getSql());

            dropTestExecutables();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTrigger() {

        try{
            List<String> ddls = createTestExecutables();

            //TODO Discuss scenario when we get an encrypted trigger, IMO display a warning,
            // it is not possible to get definition of an encrypred trigger
            DBTrigger trigger = testAdapter.getTrigger("dbo", "TriggerTestEncrypted");
            assertEquals("", trigger.getSql());
            assertEquals("1", trigger.getOptions().getChildren().get("encrypted").getData());


            dropTestExecutables();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    private String createTestView() throws Exception{
        String viewDDl =
                "CREATE VIEW dbo.testView AS\n" +
                "SELECT 'THE ALLMIGHT ''' + SC.name + ''' FROM ' + SO.name  as theBigTitle\n" +
                "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id";

        Statement stmt = testConnection.createStatement();
        stmt.execute("IF OBJECT_ID('dbo.testView', 'V') IS NOT NULL DROP VIEW dbo.testView \n");
        stmt.execute(
                "CREATE VIEW dbo.testView AS\n" +
                        "SELECT SC.name + '(' + SO.NAME + ')' as theUsualTitle\n" +
                        "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n");
        stmt.execute(
                "ALTER VIEW dbo.testView AS\n" +
                        "SELECT 'THE ALLMIGHT ''' + SC.name + ''' FROM ' + SO.name  as theBigTitle\n" +
                        "FROM sys.columns SC JOIN sys.objects SO on SC.object_id = SO.object_id\n");

        return viewDDl;

    }

    private List<String> createTestExecutables() throws Exception{

        Statement stmt = testConnection.createStatement();
        ArrayList<String> scripts = Lists.newArrayList(

            //table
            "IF OBJECT_ID('dbo.ExecutableTest', 'U') IS NOT NULL DROP TABLE dbo.ExecutableTest\n" +
            "CREATE TABLE dbo.ExecutableTest (\n" +
            "	[id] int PRIMARY KEY, \n" +
            "	[val] varchar(250) NULL DEFAULT ('Hey, I have some!')\n" +
            ") ON [PRIMARY]\n" +
            "INSERT INTO dbo.ExecutableTest VALUES (1, DEFAULT)\n",

            "IF object_id(N'dbo.FunctionTestScalar', N'FN') IS NOT NULL DROP FUNCTION dbo.FunctionTestScalar\n",

            //function scalar
            "CREATE FUNCTION dbo.FunctionTestScalar (@input VARCHAR(250), @toReplace VARCHAR(100))\n" +
            "RETURNS VARCHAR(250)\n" +
            "AS BEGIN\n" +
            "    DECLARE @Work VARCHAR(250)\n" +
            "    SET @Work = @Input\n" +
            "    SET @Work = REPLACE(@Work, @toReplace, '[' + @toReplace + ']')\n" +
            "    RETURN @work\n" +
            "END",

            //function table
            "IF object_id(N'dbo.FunctionTestTable', N'TF') IS NOT NULL DROP FUNCTION dbo.FunctionTestTable\n",

            "CREATE FUNCTION [dbo].FunctionTestTable(\n" +
            "   @find varchar(100)\n" +
            ")\n" +
            "RETURNS @Result TABLE\n" +
            "(\n" +
            "    id int ,\n" +
            "    val char(100)\n" +
            ") AS BEGIN\n" +
            "   INSERT INTO @Result SELECT * FROM dbo.ExecutableTest et WHERE et.val LIKE '%' + @find + '%'\n" +
            "   INSERT INTO @Result SELECT\n" +
            "      (SELECT MAX(ett.id) FROM dbo.ExecutableTest ett) + 1 as id, dbo.FunctionTestScalar(et.val, @find) as val FROM dbo.ExecutableTest et\n" +
            "      WHERE et.val LIKE '%' + @find + '%'\n" +
            "RETURN END",


            //procedure
            "IF OBJECT_ID('dbo.ProcedureTest', 'P') IS NOT NULL DROP PROCEDURE dbo.ProcedureTest\n",

            "CREATE PROCEDURE dbo.ProcedureTest @word varchar(100) AS\n" +
            "BEGIN\n" +
            "    SELECT *\n" +
            "    FROM dbo.FunctionTestTable(@word) t\n" +
            "END",

            //trigger
            "CREATE TRIGGER dbo.TriggerTest\n" +
            "ON dbo.ExecutableTest\n" +
            "AFTER DELETE\n" +
            "NOT FOR REPLICATION\n" +
            "AS INSERT INTO dbo.ExecutableTest SELECT * FROM deleted",

            //trigger encrypted
            "CREATE TRIGGER dbo.TriggerTestEncrypted\n" +
            "ON dbo.ExecutableTest\n" +
            "WITH ENCRYPTION\n" +
            "AFTER DELETE\n" +
            "AS INSERT INTO dbo.ExecutableTest SELECT * FROM deleted"
        );

        for (String script : scripts){
            stmt.execute(script);
        }

        return scripts;
    }

    private void dropTestExecutables() throws Exception{
        Statement stmt = testConnection.createStatement();
        stmt.execute(
            "DROP TABLE dbo.ExecutableTest\n" +
            "DROP FUNCTION dbo.FunctionTestScalar, dbo.FunctionTestTable\n" +
            "DROP PROCEDURE dbo.ProcedureTest"
        );
    }
}
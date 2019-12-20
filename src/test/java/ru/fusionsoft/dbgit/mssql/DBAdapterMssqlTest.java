package ru.fusionsoft.dbgit.mssql;

import com.google.common.collect.Lists;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.apache.commons.lang.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.oracle.DBBackupAdapterOracle;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.*;
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

    public static String TEST_CONN_URL = "localhost:1433";
    public static String TEST_CONN_STRING = "jdbc:sqlserver://"+TEST_CONN_URL+";databaseName=master;integratedSecurity=false;";
    public static String TEST_CONN_USER = "test";
    public static String TEST_CONN_PASS = "test";

    private static DBAdapterMssql testAdapter;
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


    @Before
    public void setUp() throws Exception {
        if(isInitialized) return;
        try {
            String url = testProps.getProperty("url");
            testConnection = DriverManager.getConnection(url, testProps);
            testConnection.setAutoCommit(false);
            testAdapter = (DBAdapterMssql) AdapterFactory.createAdapter(testConnection);
            isMasterDatabase = testConnection.getCatalog().equalsIgnoreCase("master");
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
        try(Statement stmt = testConnection.createStatement()){
            stmt.execute(
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
        try(Statement stmt = testConnection.createStatement()){
            stmt.execute(
            "IF EXISTS (SELECT * FROM sys.sequences WHERE NAME = N'" + name + "' AND TYPE='SO')\n" +
                "DROP Sequence " + name + "\n" +
                "CREATE Sequence " + name + "\n" +
                "START WITH 1\n" +
                "INCREMENT BY 1;\n"
            );

            DBSequence sequence = testAdapter.getSequence("dbo", name);
            assertEquals(name, sequence.getOptions().get("name").getData());
            stmt.execute("DROP Sequence " + name + "\n");
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTables() throws Exception{
        String name = "TEST_TABLE";
        String schema = testConnection.getSchema();
        String sam = schema + "." + name;

        try{
            createTable(sam,
             "PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255) "
            );

            Map<String, DBTable> tables = testAdapter.getTables(schema);
            assertEquals(name, tables.get(name).getOptions().get("name").getData());
            dropTable(sam);
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            dropTable( schema + "." + name);
        }

    }

    @Test
    public void getTableFields() throws Exception{
        String name = "TestTableTypes";
        String schema = testConnection.getSchema();
        try{
            createTable(name,
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
                "	[col20] [xml] NULL\n"
            );

            Map<String, DBTableField> fields = testAdapter.getTableFields(schema, name);
            assertEquals("sql_variant(0)", fields.get("col10").getTypeSQL());
            assertEquals("native", fields.get("col10").getTypeUniversal());

            dropTable(schema + "." + name);
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTableData() {

        try{
            StopWatch watch = new StopWatch();
            watch.start();
            createTestObjects();

            DBTableData data = testAdapter.getTableData("dbo", "ExecutableTest");
            ResultSet rs = data.getResultSet();
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            rs.next();

            assertEquals(2, cols);
            assertEquals(1, rs.getInt(1));
            assertEquals("Hey, I have some!", rs.getString(2));

            dropTestObjects();

            System.out.println(watch.toString());
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    @Test
    public void getTableDataPortion() {

        try{
            createBigDummyTable();

            int rowsAffected = 0;
            int portionSize = DBGitConfig.getInstance().getInteger( "core", "PORTION_SIZE",
                    DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000)
            );

            DBTableData data = testAdapter.getTableDataPortion("tempdb.", "#bigDummyTable", 2, 0);
            ResultSet rs = data.getResultSet();
            while (rs.next()) rowsAffected++;


            assertEquals(portionSize, rowsAffected);

            dropBigDummyTable();
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

            testConnection.createStatement().execute("DROP VIEW dbo.testView" );
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getProcedures() {

        try{
            List<String> ddls = createTestObjects();

            Map<String, DBProcedure> procedures = testAdapter.getProcedures("dbo");
            assertEquals(ddls.get(6), procedures.get("ProcedureTest").getSql());

            dropTestObjects();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getProcedure() {

        try{
            List<String> ddls = createTestObjects();

            DBProcedure procedure = testAdapter.getProcedure("dbo", "ProcedureTest");
            assertEquals(ddls.get(6), procedure.getSql());

            dropTestObjects();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getFunctions(){

        try{
            List<String> ddls = createTestObjects();

            Map<String, DBFunction> functions = testAdapter.getFunctions("dbo");
            assertEquals(ddls.get(4), functions.get("FunctionTestTable").getSql());

            dropTestObjects();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getFunction() {

        try{
            List<String> ddls = createTestObjects();

            DBFunction function = testAdapter.getFunction("dbo", "FunctionTestScalar");
            assertEquals(ddls.get(2), function.getSql());

            dropTestObjects();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTriggers() {

        try{
            List<String> ddls = createTestObjects();

            Map<String, DBTrigger> triggers = testAdapter.getTriggers("dbo");
            assertEquals(ddls.get(7), triggers.get("TriggerTest").getSql());

            dropTestObjects();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTrigger() {

        try{
            List<String> ddls = createTestObjects();

            //TODO Discuss scenario when we get an encrypted trigger, IMO display a warning,
            // it is not possible to get definition of an encrypred trigger
            DBTrigger trigger = testAdapter.getTrigger("dbo", "TriggerTestEncrypted");
            assertEquals("", trigger.getSql());
            assertEquals("1", trigger.getOptions().getChildren().get("encrypted").getData());


            dropTestObjects();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    @Test
    public void getUsers() {

        try{

            Statement stmt = testConnection.createStatement();

            String userName = "testUsr";
            String loginName = "testLgn";
            String schemaName = "public";
            String testPassHashed = "0X0200083FACBD4D7C49EAD537B1690C3E8953C504461E645D22762CED5D7CB241D87AEC312875BDB83DBD367F10C52CE7B1059056C27B8C16B083FFA97DBA2DF2F142318CCC74";
            String testPass = "test";

            createUserAndLogin(userName, loginName, schemaName, testPassHashed, true);

            Map<String, DBUser> users = testAdapter.getUsers();
            String ddl = users.get(userName).getOptions().get("ddl").getData();
            String hash = users.get(userName).getOptions().get("passwordhash").getData();

            assertTrue(users.containsKey(userName));
            assertTrue(ddl.contains(hash));
            assertTrue(ddl.contains("GRANT CONNECT"));
            assertTrue(ddl.contains("WITH DEFAULT_SCHEMA"));

            //try create connection and adapter with new login
            DBAdapterMssql adapter;
            adapter = createAdapterWithCredentials(loginName, testPass, TEST_CONN_STRING);

            //drop and close
            adapter.getConnection().close();
            dropUserAndLogin(userName, loginName);
            stmt.close();
        }
        catch (Exception ex) {
            fail(ex.toString());

        }
    }

    @Test
    public void getRoles() {

        try{

            Statement stmt = testConnection.createStatement();

            String roleName = "testRol";
            String userName = "testUsr";
            String tableSchemaAndName = "[dbo].[testTablePerm]";

            String createUserExpr =
                "CREATE USER ["+userName+"] WITHOUT LOGIN;";

            String createRoleExpr =
                "CREATE ROLE [ROLENAME];" +
                "GRANT CONTROL ON [SCHEMA].[TABLENAME] TO [ROLENAME];" +
                "GRANT DELETE ON [SCHEMA].[TABLENAME] TO [ROLENAME];" +
                "GRANT INSERT ON [SCHEMA].[TABLENAME] TO [ROLENAME];" +
                "GRANT TAKE OWNERSHIP ON [SCHEMA].[TABLENAME] TO [ROLENAME];" +
                "EXECUTE sp_AddRoleMember '"+roleName+"', '"+userName+"';";

            createRoleExpr = createRoleExpr
                .replace("[ROLENAME]", "["+roleName+"]")
                .replace("[SCHEMA].[TABLENAME]", tableSchemaAndName);

            String dropRoleExpr = "IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'"+roleName+"') DROP ROLE ["+roleName+"];";
            String dropUserExpr = "IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'"+userName+"') DROP USER ["+userName+"];";

            //create script exec
            ArrayList<String> exprs = new ArrayList<>();
            exprs.add(dropUserExpr);
            exprs.add(dropRoleExpr);
            exprs.add(createUserExpr);
            exprs.addAll(Arrays.asList(createRoleExpr.split(";")));

            createTable(tableSchemaAndName, "[someKey] int PRIMARY KEY");

            for(String expr : exprs) {
                stmt.execute(expr);
            }

            //has role test
            Map<String, DBRole> roles = testAdapter.getRoles();
            assertTrue(roles.containsKey(roleName));

            //correct ddl test
            String ddl = roles.get(roleName).getOptions().get("ddl").getData();
            assertEquals(createRoleExpr, ddl);

            stmt.execute(dropUserExpr);
            stmt.execute(dropRoleExpr);
            stmt.close();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    @Test
    public void userHasRightsToGetDdlOfOtherUsers() throws Exception{
        try{

            if(trySetMasterCatalog() && getIsDbOwner()) {
                String publicUserName = "testUsrPublic";
                String publicLoginName = "testLgnPublic";
                String password = "test";
                String dboUserName = "testUsrDbo";
                String dboLoginName = "testLgnDbo";

                createUserAndLogin(publicUserName, publicLoginName, "public", password, false);
                createUserAndLogin(dboUserName, dboLoginName, "dbo", password, false);
                addToRole(dboUserName, "db_owner");

                DBAdapterMssql publicAdapter = createAdapterWithCredentials(publicLoginName, password, TEST_CONN_STRING);
                DBAdapterMssql dboAdapter = createAdapterWithCredentials(dboLoginName, password, TEST_CONN_STRING);

                boolean publicHasRights = publicAdapter.userHasRightsToGetDdlOfOtherUsers();
                boolean dboHasRights = dboAdapter.userHasRightsToGetDdlOfOtherUsers();

                publicAdapter.getConnection().close();
                dboAdapter.getConnection().close();

                assertFalse(publicHasRights);
                assertTrue(dboHasRights);
            } else {
                System.out.println("Could not create test data, just try for not throwing exception");
            }
            testAdapter.userHasRightsToGetDdlOfOtherUsers();

        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void getDbType(){
        String type = testAdapter.getDbType();
        assertFalse(type.isEmpty());
        assertEquals(IFactoryDBConvertAdapter.MSSQL, type);
    }

    @Test
    public void getDbVersion(){
        String version = testAdapter.getDbVersion();
        assertFalse(version.isEmpty());
    }

    @Test
    public void createSchemaIfNeed() throws Exception{
        String schemaName = "TESTSCHEMA";

        testAdapter.createSchemaIfNeed(schemaName);
        assertTrue(testAdapter.getSchemes().containsKey(schemaName));

        dropSchema(schemaName);
        assertFalse(testAdapter.getSchemes().containsKey(schemaName));

    }

    @Test
    public void createRoleIfNeed() throws Exception{
        String roleName = "TESTROLETEST";

        testAdapter.createRoleIfNeed(roleName);
        assertTrue(testAdapter.getRoles().containsKey(roleName));

        dropRole(roleName);
        assertFalse(testAdapter.getRoles().containsKey(roleName));

    }

    @Test
    public void getDefaultScheme() throws Exception{
        String schemaName = testAdapter.getDefaultScheme();
        assertNotEquals("", schemaName);

    }

    @Test
    public void isReservedWord(){
        assertTrue(testAdapter.isReservedWord("NOT"));
        assertTrue(testAdapter.isReservedWord("nOt"));
        assertTrue(testAdapter.isReservedWord("CASE WHEN"));
        assertTrue(testAdapter.isReservedWord("END-EXEC"));
        assertTrue(testAdapter.isReservedWord("PERCENTILE_DISC"));
    }


    public boolean trySetMasterCatalog(){
        try {
            if (!isMasterDatabase) {
                testConnection.setCatalog("master");
            }
            return true;
        } catch (Exception e){
            System.out.println("Could not switch to master database");
            return false;
        }
    }

    public boolean getIsDbOwner() throws Exception{
        Statement stmt = testConnection.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT IS_ROLEMEMBER ('db_owner') ");

        rs.next();
        boolean isDbo = rs.getBoolean(1);

        stmt.close();
        return isDbo;
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

    public DBAdapterMssql createAdapterWithCredentials(String username, String password, String url) throws Exception{
        Properties props = new Properties();
        props.setProperty("url", Objects.nonNull(url) ? url : TEST_CONN_STRING);
        props.setProperty("user", Objects.nonNull(username) ? username : TEST_CONN_USER);
        props.setProperty("password", Objects.nonNull(password) ? password : TEST_CONN_USER);
        props.put("characterEncoding", "UTF-8");

        Connection conn = DriverManager.getConnection(props.getProperty("url"), props);
        conn.setAutoCommit(false);

        DBAdapterMssql adapter = new DBAdapterMssql();
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
                "GRANT CONNECT SQL TO ["+loginName+"];" +
                "CREATE USER ["+userName+"] FOR LOGIN ["+loginName+"] WITH DEFAULT_SCHEMA = ["+schemaName+"];"
            ).split(";")
        );

        for(String expr : createUserExprs) stmt.execute(expr);
        stmt.close();
        testConnection.commit();
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

    public void createTable(String schemaAndName, String fieldsExpr) throws Exception{

        try (Statement stmt = testConnection.createStatement()){
            String name = convertSchemaAndName(schemaAndName);
            stmt.execute("IF OBJECT_ID('"+name+"', 'U') IS NOT NULL DROP TABLE " + name);
            stmt.execute("CREATE TABLE "+schemaAndName+"( " +fieldsExpr+" ) ON [PRIMARY]\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropTable(String schemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();
        String name = convertSchemaAndName(schemaAndName);
        stmt.execute("DROP TABLE "+name);
        stmt.close();
    }

    public String convertSchemaAndName(String san) {
        return san.startsWith("#")
            ? "tempdb.." + san.substring(1)
            : san;
    }

    //entire sets

    public void createBigDummyTable() throws Exception{

        Statement stmt = testConnection.createStatement();
        List<String> scripts = Lists.newArrayList(
    "IF OBJECT_ID('tempdb..#bigDummyTable', 'U') IS NOT NULL DROP TABLE #bigDummyTable\n",

            "WITH e1(n) AS (   \n" +
            "   SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL \n" +
            "    SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL \n" +
            "    SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 ), -- 10\n" +
            "e2(n) AS (SELECT 1 FROM e1 CROSS JOIN e1 b), -- 10*10\n" +
            "e3(n) AS (SELECT 1 FROM e1 CROSS JOIN e2), -- 10*100\n" +
            "e4(n) AS (SELECT 1 FROM e1 CROSS JOIN e3), -- 10*1000\n" +
            "e5(n) AS (SELECT 1 FROM e1 CROSS JOIN e4) -- 10*10000\n" +
            "SELECT n1 = ROW_NUMBER() OVER (ORDER BY n) \n" +
            "INTO #bigDummyTable FROM e5 ORDER BY n1"
        );

        for (String script : scripts){
            stmt.execute(script);
        }
        stmt.close();
    }

    public void dropBigDummyTable() throws Exception{
        
        Statement stmt = testConnection.createStatement();
        try { stmt.execute("DROP TABLE tempdb..#bigDummyTable\n"); } catch (SQLServerException ex) {
            ConsoleWriter.println("Failed to drop #bigDummyTable");
        }
        stmt.close();
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

    private List<String> createTestObjects() throws Exception{

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

    private void dropTestObjects() throws Exception{
        Statement stmt = testConnection.createStatement();
        stmt.execute(
            "DROP TABLE dbo.ExecutableTest\n" +
            "DROP FUNCTION dbo.FunctionTestScalar, dbo.FunctionTestTable\n" +
            "DROP PROCEDURE dbo.ProcedureTest"
        );
    }
}
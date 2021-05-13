package ru.fusionsoft.dbgit.mssql;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.time.StopWatch;
import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;


@Tag("mssqlTest")
@Tag("deprecated")
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
    */

    public static String TEST_CONN_URL = "23.105.226.179:1433";
    public static String TEST_CONN_CATALOG = "testdatabasegit";
    public static String TEST_CONN_STRING = "jdbc:sqlserver://"+TEST_CONN_URL+";databaseName="+TEST_CONN_CATALOG+";integratedSecurity=false;";
    public static String TEST_CONN_USER = "sa";
    public static String TEST_CONN_PASS = "s%G351as";

    private static DBAdapterMssql testAdapter;
    private static DBBackupAdapterMssql testBackup;
    private static FactoryDBAdapterRestoreMssql testRestoreFactory = new FactoryDBAdapterRestoreMssql();
    private static DBRestoreTriggerMssql restoreTrigger;


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
    private static int messageLevel = 0;


    @BeforeEach
    public void setUp() throws Exception {
        if(!isInitialized){
            try {
                String url = testProps.getProperty("url");
                testConnection = DriverManager.getConnection(url, testProps);
                testConnection.setAutoCommit(false);
                testAdapter = (DBAdapterMssql) AdapterFactory.createAdapter(testConnection);
                testBackup = (DBBackupAdapterMssql) testAdapter.getBackupAdapterFactory().getBackupAdapter(testAdapter);
                restoreTrigger = (DBRestoreTriggerMssql) testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitTrigger,testAdapter);
                isMasterDatabase = testConnection.getCatalog().equalsIgnoreCase("master");
                schema = testConnection.getSchema();
                isInitialized = true;
            }
            catch (Exception ex){
                fail(ex.getMessage());
            }
            dropBackupObjects();
        }
    }

    @AfterEach
    public void tearDown() throws Exception
    {
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
    public void getSequences() throws Exception{
        String schemaName = testConnection.getSchema();
        String sequenceName = "TEST_SEQUENCE";
        createTestSequence(sequenceName);

        Map<String, DBSequence> sequences = testAdapter.getSequences(schemaName);
        dropTestSequence(sequenceName);

        assertEquals(schemaName, sequences.get(sequenceName).getOptions().get("owner").getData());

    }

    @Test
    public void getSequence() throws Exception{
        String name = "TEST_SEQUENCE";

        createTestSequence(name);
        DBSequence sequence = testAdapter.getSequence("dbo", name);
        dropTestSequence(name);

        assertEquals(name, sequence.getOptions().get("name").getData());
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
        } catch (Exception ex) {
            fail(ex.toString());
        } finally {
            dropTable(sam);
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
            assertEquals("native",fields.get("col10").getTypeUniversal().toString());

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
            createTestTriggerProcedureFunctions(triggerTableName);

            DBTableData data = testAdapter.getTableData(testConnection.getSchema(), triggerTableName);
            ResultSet rs = data.resultSet();
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            rs.next();

            assertEquals(2, cols);
            assertEquals(1, rs.getInt(1));
            assertEquals("Hey, I have some!", rs.getString(2));

            dropTestTriggerProcedureFunctions(triggerTableName);

            System.out.println(watch.toString());
        }
        catch (Exception ex) {
            fail(ex.getLocalizedMessage());
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
            ResultSet rs = data.resultSet();
            while (rs.next()) rowsAffected++;


            assertEquals(portionSize, rowsAffected);

            dropBigDummyTable();
        }
        catch (Exception ex) {
            fail(ex.toString());
        }
    }

    @Test
    public void getIndexes() throws Exception{

        String tableName = "TestTableIndex";

        String indexCreateDdl = createTestIndex(tableName);

        Map<String, DBIndex> indexes = testAdapter.getIndexes(testConnection.getSchema(), tableName);
        assertEquals("2", indexes.get("IX_IdTest").getOptions().getChildren().get("indexid").getData());
        assertEquals(indexCreateDdl, indexes.get("IX_IdTest").getSql());

        dropTestIndex(tableName);

    }

    @Test
    public void getConstraints() throws Exception{

        String schema = testConnection.getSchema();
        String tableName = "CTestTable";
        String tableSam = schema + "." + tableName;

        String constrDDL1 = "ALTER TABLE "+ tableSam + " ADD CONSTRAINT df_constraint DEFAULT ('{}') FOR [value];";
        String constrDDL2 = "ALTER TABLE "+ tableSam + " ADD CONSTRAINT df_constraintInt DEFAULT ((1)) FOR [valueCheck1];";
        String constrDDL3 = "ALTER TABLE "+ tableSam + " ADD CONSTRAINT u_constraint UNIQUE NONCLUSTERED ([valueUnique]);";
        String constrDDL4 = "ALTER TABLE "+ tableSam + " ADD CONSTRAINT chk_constraint CHECK ([valueCheck1]>(0) AND [valueCheck2]>(0));";
        String constrDDL5 = "ALTER TABLE "+ tableSam + " ADD CONSTRAINT fk_constraint FOREIGN KEY (fkInt) references " + tableSam + "FK(keyInt);";


        createTestConstraintsAndTables(schema, tableName);


        Map<String, DBConstraint> constraints = testAdapter.getConstraints(schema, tableName);
        assertEquals(constrDDL1, constraints.get("df_constraint").getSql());
        assertEquals(constrDDL2, constraints.get("df_constraintInt").getSql());
        assertEquals(constrDDL3, constraints.get("u_constraint").getSql());
        assertEquals(constrDDL4, constraints.get("chk_constraint").getSql());
        assertEquals(constrDDL5, constraints.get("fk_constraint").getSql());

        dropTestConstraintsAndTables(schema, tableName);

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
    public void getView() throws Exception {
        String schema = testConnection.getSchema();
        String viewName = "TestView";
        String viewDDl = createTestView(schema, viewName);

        DBView view = testAdapter.getView(schema, viewName);
        assertEquals(viewDDl, view.getSql());

        dropTestView(schema, viewName);
    }

    @Test
    public void getProcedures() {

        try{
            List<String> ddls = createTestTriggerProcedureFunctions(triggerTableName);

            Map<String, DBProcedure> procedures = testAdapter.getProcedures(testConnection.getSchema());
            assertEquals(ddls.get(6), procedures.get(procedureName).getSql());

            dropTestTriggerProcedureFunctions(triggerTableName);

        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getProcedure() {

        try{
            List<String> ddls = createTestTriggerProcedureFunctions(triggerTableName);;

            DBProcedure procedure = testAdapter.getProcedure(testConnection.getSchema(), procedureName);
            assertEquals(ddls.get(6), procedure.getSql());

            dropTestTriggerProcedureFunctions(triggerTableName);
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getFunctions(){

        try{
            List<String> ddls = createTestTriggerProcedureFunctions(triggerTableName);;

            Map<String, DBFunction> functions = testAdapter.getFunctions(testConnection.getSchema());
            assertEquals(ddls.get(4), functions.get(functionNameTable).getSql());

            dropTestTriggerProcedureFunctions(triggerTableName);
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getFunction() {

        try{
            List<String> ddls = createTestTriggerProcedureFunctions(triggerTableName);;

            DBFunction function = testAdapter.getFunction(testConnection.getSchema(), functionName);
            assertEquals(ddls.get(2), function.getSql());

            dropTestTriggerProcedureFunctions(triggerTableName);
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTriggers() {

        try{
            List<String> ddls = createTestTriggerProcedureFunctions(triggerTableName);;

            Map<String, DBTrigger> triggers = testAdapter.getTriggers(testConnection.getSchema());
            assertEquals(ddls.get(7), triggers.get(triggerName).getSql());

            dropTestTriggerProcedureFunctions(triggerTableName);
        }
        catch (Exception ex) {
            fail(ex.toString());
        }

    }

    @Test
    public void getTrigger() {

        try{
            List<String> ddls = createTestTriggerProcedureFunctions(triggerTableName);;

            //TODO Discuss scenario when we get an encrypted trigger, IMO display a warning,
            // it is not possible to get definition of an encrypred trigger
            DBTrigger trigger = testAdapter.getTrigger("dbo", triggerName + "Encrypted");
            assertEquals("", trigger.getSql());
            assertEquals("1", trigger.getOptions().getChildren().get("encrypted").getData());


            dropTestTriggerProcedureFunctions(triggerTableName);
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
            fail(ex.getMessage());

        }
    }

    @Test
    public void getRoles() throws Exception{

        String roleName = "testRol";
        String userName = "testUsr";
        String tableSchemaAndName = "[dbo].[testTablePerm]";
        String roleDdl = createTestRoleAndStuff(roleName, userName, tableSchemaAndName);


        Map<String, DBRole> roles = testAdapter.getRoles();
        dropTestRoleAndStuff(roleName, userName, tableSchemaAndName);

        //has role test
        assertTrue(roles.containsKey(roleName));

        //correct ddl test
        String ddl = roles.get(roleName).getOptions().get("ddl").getData();
        assertEquals(roleDdl, ddl);

    }

    @Test
    public void userHasRightsToGetDdlOfOtherUsers() throws Exception{
        try{

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

            testAdapter.userHasRightsToGetDdlOfOtherUsers();

        }
        catch (Exception e){
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void getDbType(){
        DbType type = testAdapter.getDbType();
        assertEquals(DbType.MSSQL, type);
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

    //backupAdapter methods

    @Test
    public void backupMetaSequence() throws Exception{
        try(AutoCloseable testSequence = useTestSequence(sequenceName)){

            MetaSequence metaSequence = new MetaSequence(testAdapter.getSequences(schema).get(sequenceName));
            testBackup.backupDBObject(metaSequence);
        }
    }

    @Test
    public void backupMetaTable() throws Exception{
        String tableName = "CTestTable";
        String indexTableName = "TestTableIndex";
        String sam = schema + ".PersonsTest";

        String someTableDdl = createTable(sam, "PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255) ");
        createTestIndex(indexTableName);
        createTestConstraintsAndTables(schema, tableName);

        Map<String, DBTable> tables = testAdapter.getTables(schema);

        MetaTable constraintsMetaTable = new MetaTable(tables.get(tableName));
        MetaTable indexedMetaTable = new MetaTable(tables.get(indexTableName));
        testConnection.commit();

        testBackup.backupDBObject(constraintsMetaTable);
        testBackup.backupDBObject(indexedMetaTable);

        dropTable(sam);
        dropTestIndex(indexTableName);
        dropTestConstraintsAndTables(schema, tableName);
        testConnection.commit();
    }

    @Test
    public void backupMetaSql() throws Exception{
        dropBackupObjects();
        createTestTriggerProcedureFunctions(triggerTableName);
        createTestView(schema, viewName);

        MetaView metaView = new MetaView(testAdapter.getViews(schema).get(viewName));
        MetaTrigger metaTrigger = new MetaTrigger(testAdapter.getTriggers(schema).get(triggerName));
        MetaProcedure metaProcedure = new MetaProcedure(testAdapter.getProcedures(schema).get(procedureName));
        MetaFunction metaFunction = new MetaFunction(testAdapter.getFunctions(schema).get(functionName));
        MetaFunction metaFunctionTable = new MetaFunction(testAdapter.getFunctions(schema).get(functionName+"Table"));

        testBackup.backupDBObject(metaView);
        testBackup.backupDBObject(metaTrigger);
        testBackup.backupDBObject(metaProcedure);
        testBackup.backupDBObject(metaFunction);
        testBackup.backupDBObject(metaFunctionTable);

        dropTestView(schema, viewName);
        dropTestTriggerProcedureFunctions(triggerTableName);
    }

    @Test
    public void isExists() throws Exception{
        String objectName = "ShouldExist";

        try(AutoCloseable view = useTestView(schema, objectName)){
            assertTrue(testBackup.isExists(schema, objectName));
        }
    }

    @Test
    public void createSchema() throws Exception {
        String schemaName = "TESTSCHEMA2";
        String prefix = "BACKUP$";
        StatementLogging stLog = new StatementLogging(testConnection, testAdapter.getStreamOutputSqlCommand(), testAdapter.isExecSql());

        testBackup.createSchema(stLog, schemaName);
        assertTrue(testAdapter.getSchemes().containsKey(prefix+schemaName));
        dropSchema(prefix+schemaName);
    }

    public void restoreDBObject() throws Exception{

    }
    public void backupMetaObjOptions() throws Exception{
        String roleName = "TestRoleB";
        String userName = "TestUserB";
        String tableName = "RTestTable";
        String sam = schema + "." + tableName;
        createTestRoleAndStuff(roleName, userName, sam);

        MetaRole metaRole = new MetaRole(testAdapter.getRoles().get(roleName));
        MetaUser metaUser = new MetaUser(testAdapter.getUsers().get(userName));
        MetaSchema metaSchema = new MetaSchema(testAdapter.getSchemes().get(schema));
        MetaTableSpace metaTableSpace = new MetaTableSpace(testAdapter.getTableSpaces().get("PRIMARY"));

        List<MetaObjOptions> objs = Arrays.asList(metaRole, metaUser, metaSchema, metaTableSpace);
        for(MetaObjOptions obj : objs) testBackup.backupDBObject(obj);

        dropTestRoleAndStuff(roleName, userName, sam);
    }

    //restoreAdapter methods

    @Test
    public void getAdapterRestore(){
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitSequence,testAdapter).getClass(),
                DBRestoreSequenceMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitTable,testAdapter).getClass(),
                DBRestoreTableMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitTableData,testAdapter).getClass(),
                DBRestoreTableDataMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitSchema,testAdapter).getClass(),
                DBRestoreSchemaMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitFunction,testAdapter).getClass(),
                DBRestoreFunctionMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitProcedure,testAdapter).getClass(),
                DBRestoreProcedureMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitRole,testAdapter).getClass(),
                DBRestoreRoleMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitTrigger,testAdapter).getClass(),
                DBRestoreTriggerMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitView,testAdapter).getClass(),
                DBRestoreViewMssql.class
        );
        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitPackage,testAdapter),
                null
        );

        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitTableSpace,testAdapter).getClass(),
                DBRestoreTableSpaceMssql.class
        );

        assertEquals(
                testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitUser,testAdapter).getClass(),
                DBRestoreUserMssql.class
        );

    }

    @Test
    public void restoreSequence() throws Exception{
        String seqName = "testSeq";
        String seqDdl = createTestSequence(seqName);

        DBRestoreSequenceMssql ra = (DBRestoreSequenceMssql) testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitSequence,testAdapter);
        DBSequence dbSequence = testAdapter.getSequence(schema, seqName);
        MetaSequence metaSequence = new MetaSequence(dbSequence);
        DbType sourceDbType = ra.getSourceDbType(metaSequence);

        dropTestSequence(seqName);
        assertFalse(testAdapter.getSequences(schema).containsKey(seqName));

        ra.restoreMetaObject(metaSequence);
        assertTrue(testAdapter.getSequences(schema).containsKey(seqName));

        dropTestSequence(seqName);
    }

    @Test
    public void restoreTable() throws Exception{
        DBRestoreTableMssql restoreAdapter = (DBRestoreTableMssql) testRestoreFactory.getAdapterRestore(DBGitMetaType.DBGitTable,testAdapter);


        String tblNameConstraints = "testConstraintsTable";
        createTestConstraintsAndTables(tblNameConstraints);
        MetaTable metaTableConstraints = new MetaTable(testAdapter.getTable(schema, tblNameConstraints));
        metaTableConstraints.loadFromDB();

        String tblNameIndexes = "testTbl";
        createTestIndex(tblNameIndexes);
        MetaTable metaTableIndexes = new MetaTable(testAdapter.getTable(schema, tblNameIndexes));
        metaTableIndexes.loadFromDB();

        Map<String, DBConstraint> constraintsBefore = metaTableConstraints.getConstraints();
        Map<String, DBIndex> indexesBefore = metaTableIndexes.getIndexes();

        for(MetaTable mt : Arrays.asList(metaTableConstraints, metaTableIndexes)){
            String tableName = mt.getTable().getName();
            dropTable(schema, tableName);
            assertFalse(testAdapter.getTables(schema).containsKey(tableName));

            for(int step : Arrays.asList(0, -1, 1)){
                restoreAdapter.restoreMetaObject(mt, step);
            }
            assertTrue(testAdapter.getTables(schema).containsKey(tableName));
        }

        Map<String, DBConstraint> constraintsAfter = testAdapter.getConstraints(schema, tblNameConstraints);
        Map<String, DBIndex> indexesAfter = testAdapter.getIndexes(schema, tblNameIndexes);

        assertTrue(indexesBefore.keySet().containsAll(indexesAfter.keySet()));
        assertTrue(constraintsBefore.keySet().containsAll(constraintsAfter.keySet()));

        dropTestConstraintsAndTables(tblNameConstraints);
        dropTestIndex(tblNameIndexes);

        testConnection.commit();

    }

    @Test
    public void restoreFunction() throws Exception{
        DBRestoreFunctionMssql restoreAdapter = (DBRestoreFunctionMssql) testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitFunction,testAdapter);
        String tableName = "someTestTable";

        createTestTriggerProcedureFunctions(tableName);
        MetaFunction metaFunction = new MetaFunction(testAdapter.getFunction(schema, functionName));
        MetaFunction metaFunctionTable = new MetaFunction(testAdapter.getFunction(schema, functionNameTable));
        metaFunction.loadFromDB();
        metaFunctionTable.loadFromDB();
        assertTrue(testAdapter.getFunctions(schema).containsKey(functionName));
        assertTrue(testAdapter.getFunctions(schema).containsKey(functionNameTable));

        restoreAdapter.restoreMetaObject(metaFunction);
        restoreAdapter.restoreMetaObject(metaFunctionTable);

        dropTestTriggerProcedureFunctions(tableName);
        assertFalse(testAdapter.getFunctions(schema).containsKey(functionName));
        assertFalse(testAdapter.getFunctions(schema).containsKey(functionNameTable));


        restoreAdapter.restoreMetaObject(metaFunction);
        restoreAdapter.restoreMetaObject(metaFunctionTable);
        assertTrue(testAdapter.getFunctions(schema).containsKey(functionName));
        assertTrue(testAdapter.getFunctions(schema).containsKey(functionNameTable));
    }

    @Test
    public void restoreProcedure() throws Exception{
        DBRestoreProcedureMssql restoreAdapter = (DBRestoreProcedureMssql) testRestoreFactory.getAdapterRestore(DBGitMetaType.DbGitProcedure,testAdapter);
        String tableName = "someTestTable";

        createTestTriggerProcedureFunctions(tableName);
        MetaProcedure metaProcedure = new MetaProcedure(testAdapter.getProcedure(schema, procedureName));
        metaProcedure.loadFromDB();
        restoreAdapter.restoreMetaObject(metaProcedure);
        assertTrue(testAdapter.getProcedures(schema).containsKey(procedureName));

        dropTestTriggerProcedureFunctions(tableName);
        assertFalse(testAdapter.getProcedures(schema).containsKey(procedureName));

        restoreAdapter.restoreMetaObject(metaProcedure);
        assertTrue(testAdapter.getProcedures(schema).containsKey(procedureName));
    }

    @Test
    public void restoreTriggerEncryptedNotExist() throws Exception{
        List<String> ddls = createTestTriggerProcedureFunctions(tableName);
        assertTrue(testAdapter.getTriggers(schema).containsKey(triggerNameEncr));

        MetaTrigger metaTriggerEncr = new MetaTrigger(testAdapter.getTrigger(schema, triggerNameEncr));
        metaTriggerEncr.loadFromDB();

        dropTestTriggerProcedureFunctions(tableName);
        restoreTrigger.restoreMetaObject(metaTriggerEncr);

        assertFalse(testAdapter.getTriggers(schema).containsKey(triggerNameEncr));
    }

    @Test
    public void restoreTriggerExisting() throws Exception{
        List<String> ddls = createTestTriggerProcedureFunctions(tableName);
        assertTrue(testAdapter.getTriggers(schema).containsKey(triggerName));

        MetaTrigger metaTrigger = new MetaTrigger(testAdapter.getTrigger(schema, triggerName));
        metaTrigger.loadFromDB();

        restoreTrigger.restoreMetaObject(metaTrigger);

        assertTrue(testAdapter.getTriggers(schema).containsKey(triggerName));
    }

    @Test
    public void restoreTriggerAltered() throws Exception{
        String triggerDdl = createTestTriggerProcedureFunctions(tableName).get(7);
        assertTrue(testAdapter.getTriggers(schema).containsKey(triggerName));

        MetaTrigger metaTrigger = new MetaTrigger(testAdapter.getTrigger(schema, triggerName));
        metaTrigger.loadFromDB();

        try(Statement st = testConnection.createStatement()){
            st.execute(MessageFormat.format(
                "ALTER TRIGGER {0}.{1} ON {2} {3}",
                schema, triggerName, tableName, "AFTER UPDATE AS RAISERROR ('An Update is performed on the "+tableName+" table', 0, 0);"
            ));
        }

        restoreTrigger.restoreMetaObject(metaTrigger);

        assertTrue(testAdapter.getTriggers(schema).containsKey(triggerName));
        assertTrue(testAdapter.getTrigger(schema, triggerName).getSql().equals(triggerDdl));
    }

    @Test
    public void restoreTriggerAfterDrop() throws Exception {
        List<String> ddls = createTestTriggerProcedureFunctions(tableName);
        String triggerDdl = ddls.get(7);
        String tableCreateDdl = ddls.get(0);
        assertTrue(testAdapter.getTriggers(schema).containsKey(triggerNameEncr));

        MetaTrigger metaTrigger = new MetaTrigger(testAdapter.getTrigger(schema, triggerName));
        metaTrigger.loadFromDB();

        try(Statement st = testConnection.createStatement()){
            st.execute("DROP TRIGGER " + triggerName);
        }
        restoreTrigger.restoreMetaObject(metaTrigger);

        assertTrue(testAdapter.getTriggers(schema).containsKey(triggerName));
        assertTrue(testAdapter.getTrigger(schema, triggerName).getSql().equals(triggerDdl));
    }

    //heplers

    public void dropBackupObjects() throws Exception{
        Map<String, DBTable> tables = testAdapter.getTables(schema);
        for (DBTable table : tables.values()){
            if(table.getName().startsWith("BACKUP$")){
                ConsoleWriter.println("drop "+table.getName(), messageLevel);
                dropTable(schema+"."+table.getName());
            }
        }

        Map<String, DBView> views = testAdapter.getViews(schema);
        for (DBView view : views.values()){
            if(view.getName().startsWith("BACKUP$")){
                ConsoleWriter.println("drop "+view.getName(), messageLevel);
                dropView(schema+"."+view.getName());
            }
        }

        Map<String, DBTrigger> Triggers = testAdapter.getTriggers(schema);
        for (DBTrigger Trigger : Triggers.values()){
            if(Trigger.getName().startsWith("BACKUP$")){
                ConsoleWriter.println("drop "+Trigger.getName(), messageLevel);
                dropTrigger(schema+"."+Trigger.getName());
            }
        }

        Map<String, DBProcedure> Procedures = testAdapter.getProcedures(schema);
        for (DBProcedure Procedure : Procedures.values()){
            if(Procedure.getName().startsWith("BACKUP$")){
                ConsoleWriter.println("drop "+Procedure.getName(), messageLevel);
                dropProcedure(schema+"."+Procedure.getName());
            }
        }

        Map<String, DBFunction> Functions = testAdapter.getFunctions(schema);
        for (DBFunction Function : Functions.values()){
            if(Function.getName().startsWith("BACKUP$")){
                ConsoleWriter.println("drop "+Function.getName(), messageLevel);
                dropFunction(schema+"."+Function.getName());
            }
        }


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

    public boolean trySetInnitialCatalog(){
        try {
            testConnection.setCatalog(TEST_CONN_CATALOG);
            return true;
        } catch (Exception e){
            System.out.println("Could not switch to master database");
            return false;
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

    public void dropView(String schemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();
        String name = convertSchemaAndName(schemaAndName);
        String ddl = MessageFormat.format("IF OBJECT_ID(''{0}'', ''V'') IS NOT NULL DROP VIEW {0}", name);
        stmt.execute(ddl);
        stmt.close();
    }

    public void dropTrigger(String schemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();
        String name = convertSchemaAndName(schemaAndName);
        String ddl = MessageFormat.format("IF OBJECT_ID(''{0}'', ''TR'') IS NOT NULL DROP TRIGGER {0}", name);
        stmt.execute(ddl);
        stmt.close();
    }

    public void dropProcedure(String schemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();
        String name = convertSchemaAndName(schemaAndName);
        String ddl = MessageFormat.format("IF OBJECT_ID(''{0}'', ''P'') IS NOT NULL DROP PROCEDURE {0}", name);
        stmt.execute(ddl);
        stmt.close();
    }

    public void dropFunction(String schemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();
        String name = convertSchemaAndName(schemaAndName);
        String ddl1 = MessageFormat.format("IF OBJECT_ID(''{0}'', ''FN'') IS NOT NULL DROP FUNCTION {0}", name);
        String ddl2 = MessageFormat.format("IF OBJECT_ID(''{0}'', ''IF'') IS NOT NULL DROP FUNCTION {0}", name);
        String ddl3 = MessageFormat.format("IF OBJECT_ID(''{0}'', ''TF'') IS NOT NULL DROP FUNCTION {0}", name);
        stmt.execute(ddl1);
        stmt.execute(ddl2);
        stmt.execute(ddl3);
        stmt.close();
    }

    public String convertSchemaAndName(String san) {
        return san.startsWith("#")
            ? "tempdb.." + san.substring(1)
            : san;
    }

    //entire sets

    private void createTestConstraintsAndTables(String tableName) throws SQLException {
        createTestConstraintsAndTables(schema, tableName);
    }

    private void createTestConstraintsAndTables(String schema, String tableName) throws SQLException {
        String constrDDL1 = "ALTER TABLE "+ schema + "." + tableName + " ADD CONSTRAINT df_constraint DEFAULT ('{}') FOR [value];";
        String constrDDL2 = "ALTER TABLE "+ schema + "." + tableName + " ADD CONSTRAINT df_constraintInt DEFAULT ((1)) FOR [valueCheck1];";
        String constrDDL3 = "ALTER TABLE "+ schema + "." + tableName + " ADD CONSTRAINT u_constraint UNIQUE NONCLUSTERED ([valueUnique]);";
        String constrDDL4 = "ALTER TABLE "+ schema + "." + tableName + " ADD CONSTRAINT chk_constraint CHECK (valueCheck1>(0) AND valueCheck2>(0));";
        String constrDDL5 = "ALTER TABLE "+ schema + "." + tableName + " ADD CONSTRAINT fk_constraint FOREIGN KEY (fkInt) references " + schema + "." + tableName + "FK(keyInt);";

        Statement stmt = testConnection.createStatement();
        stmt.execute(
        "IF OBJECT_ID('"+ schema + "." + tableName + "', 'U') IS NOT NULL DROP TABLE "+ schema + "." + tableName + ";\n" +

            "CREATE TABLE "+ schema + "." + tableName + " (\n" +
            "	[key] varchar(20) PRIMARY KEY, \n" +
            "	[value] varchar(20) NOT NULL, \n" +
            "   [valueCheck1] int NOT NULL, \n" +
            "   [valueCheck2] int NOT NULL, \n" +
            "	[valueUnique] varchar(20),\n" +
            "	[fkInt] int\n" +
            ") ON [PRIMARY];\n" +

            "IF OBJECT_ID('"+ schema + "." + tableName + "FK', 'U') IS NOT NULL DROP TABLE "+ schema + "." + tableName + "FK ;\n" +

            "CREATE TABLE "+ schema + "." + tableName + "FK ( keyInt int PRIMARY KEY, valueChar nvarchar(100) );\n"
        );

        stmt.execute(constrDDL1 + constrDDL2 + constrDDL3 + constrDDL4 + constrDDL5);
        stmt.close();
    }

    private void dropTestConstraintsAndTables(String tableName) throws Exception {dropTestConstraintsAndTables(schema, tableName);}
    private void dropTestConstraintsAndTables(String schema, String tableName) throws Exception {
        dropTable(schema+"."+tableName);
        dropTable(schema+"."+tableName+"FK");
//        testConnection.commit();
    }

    public AutoCloseable useTestSequence(String sequenceName) throws Exception{
        return new AutoCloseable() {
            {
                createTestSequence(sequenceName);
            }

            @Override
            public void close() throws Exception {
                dropTestSequence(sequenceName);
            }
        };
    }


    private String createTestSequence(String sequenceName) throws SQLException {
        String dieDdl =
            "IF EXISTS (SELECT * FROM sys.sequences WHERE NAME = N'"+sequenceName+"' AND TYPE='SO')\n" +
            "DROP Sequence "+sequenceName+"\n";
        String crDdl =
            "CREATE SEQUENCE "+sequenceName+"\n" +
            "START WITH 1\n" +
            "INCREMENT BY 1;\n";

        try(Statement stmt = testConnection.createStatement()){
            stmt.execute(dieDdl + crDdl);
        }
        return crDdl;
    }

    public void dropTestSequence(String sequenceName) throws Exception{
        Statement stmt = testConnection.createStatement();
        stmt.execute(
        "IF EXISTS (SELECT * FROM sys.sequences WHERE NAME = N'"+sequenceName+"' AND TYPE='SO')\n" +
            "DROP Sequence "+sequenceName+"\n"
        );
        stmt.close();
    }

    public String createTestRoleAndStuff(String roleName, String userName, String tableSchemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();

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

        //build execution pipeline
        ArrayList<String> exprs = new ArrayList<>();
        exprs.add(dropUserExpr);
        exprs.add(dropRoleExpr);
        exprs.add(createUserExpr);
        exprs.addAll(Arrays.asList(createRoleExpr.split(";")));


        //table for testing permissions
        createTable(tableSchemaAndName, "[someKey] int PRIMARY KEY");


        //execute scripts
        for(String expr : exprs) {
            stmt.execute(expr);
        }

        stmt.close();

        return createRoleExpr;
    }

    public void dropTestRoleAndStuff(String roleName, String userName, String tableSchemaAndName) throws Exception{
        Statement stmt = testConnection.createStatement();

        String dropRoleExpr = "IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'"+roleName+"') DROP ROLE ["+roleName+"];";
        String dropUserExpr = "IF EXISTS (SELECT * FROM sys.database_principals WHERE name = N'"+userName+"') DROP USER ["+userName+"];";

        stmt.execute(dropUserExpr);
        stmt.execute(dropRoleExpr);
        stmt.close();

        dropTable(tableSchemaAndName);
    }

    public String createTestIndex(String tableName) throws Exception{
        String schema = testConnection.getSchema();
        return createTestIndex(schema, tableName);
    }

    public String createTestIndex(String schemaName, String tableName) throws Exception{
        dropTestIndex(schemaName, tableName);
        String indexCreateDdl = "CREATE NONCLUSTERED INDEX [IX_IdTest] ON ["+schema+"].["+tableName+"] ([Id], [Name]) ON [PRIMARY];";
        createTable(
schema + "." + tableName,
    "	[Id] [nvarchar](128) NOT NULL, " +
            "	[Name] [nvarchar](256) NOT NULL, " +
            "   CONSTRAINT [TestConstraint] PRIMARY KEY CLUSTERED \n" +
            "       ([Id] ASC) " +
            "       WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]\n"
        );

        Statement stmt = testConnection.createStatement();
        stmt.execute(indexCreateDdl);
        stmt.close();
        return indexCreateDdl;
    }

    public void dropTestIndex(String tableName)throws Exception{ dropTestIndex(schema, tableName); }

    public void dropTestIndex(String schema, String tableName)throws Exception{
        Statement stmt = testConnection.createStatement();

        dropTable(schema + "." + tableName);
        String ddl =
            "IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'"+schema+"."+tableName+"') AND name = 'IX_IdTest') " +
            "DROP INDEX IX_IdTest ON " + schema + "." + tableName + "\n";
        stmt.execute(ddl);
        stmt.close();
    }

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
        try { stmt.execute("DROP TABLE tempdb..#bigDummyTable\n"); } catch (SQLException ex) {
            ConsoleWriter.println("Failed to drop #bigDummyTable", messageLevel);
        }
        stmt.close();
    }

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

    private List<String> createTestTriggerProcedureFunctions(String tableName) throws Exception{

        Statement stmt = testConnection.createStatement();
        String schema = testConnection.getSchema();
        String sam = schema+"."+tableName;
        String functionSam = schema + "." + functionName;
        String functionSamTable = functionSam + "Table";
        String functionSamScalar = functionSam;
        String procedureSam = MessageFormat.format("{0}.{1}", schema, procedureName);
        String triggerSam = MessageFormat.format("{0}.{1}", schema, triggerName);
        String triggerSamEncrypted = MessageFormat.format("{0}.{1}", schema, triggerNameEncr);

        ArrayList<String> scripts = Lists.newArrayList(

            // (0) table
            "IF OBJECT_ID('"+sam+"', 'U') IS NOT NULL DROP TABLE "+sam+"\n" +
            "CREATE TABLE "+sam+" (\n" +
            "	[id] int PRIMARY KEY, \n" +
            "	[val] varchar(250) NULL DEFAULT ('Hey, I have some!')\n" +
            ") ON [PRIMARY]\n" +
            "INSERT INTO "+sam+" VALUES (1, DEFAULT)\n",

            // (1) drop if exists function scalar
            "IF object_id(N'"+functionSamScalar+"', N'FN') IS NOT NULL DROP FUNCTION "+functionSamScalar+"\n",

            // (2) function scalar  ddl
            "CREATE FUNCTION "+functionSamScalar+" (@input VARCHAR(250), @toReplace VARCHAR(100))\n" +
            "RETURNS VARCHAR(250)\n" +
            "AS BEGIN\n" +
            "    DECLARE @Work VARCHAR(250)\n" +
            "    SET @Work = @Input\n" +
            "    SET @Work = REPLACE(@Work, @toReplace, '[' + @toReplace + ']')\n" +
            "    RETURN @work\n" +
            "END",

            // (3) drop if exists function table
            "IF object_id(N'"+functionSamTable+"', N'TF') IS NOT NULL DROP FUNCTION "+functionSamTable+"\n",

            // (4) function table ddl
            "CREATE FUNCTION "+functionSamTable+"(\n" +
            "   @find varchar(100)\n" +
            ")\n" +
            "RETURNS @Result TABLE\n" +
            "(\n" +
            "    id int ,\n" +
            "    val char(100)\n" +
            ") AS BEGIN\n" +
            "   INSERT INTO @Result SELECT * FROM "+sam+" et WHERE et.val LIKE '%' + @find + '%'\n" +
            "   INSERT INTO @Result SELECT\n" +
            "      (SELECT MAX(ett.id) FROM "+sam+" ett) + 1 as id, "+functionSamScalar+"(et.val, @find) as val FROM "+sam+" et\n" +
            "      WHERE et.val LIKE '%' + @find + '%'\n" +
            "RETURN END",

            // (5) drop if exists procedure and triggers
            "IF OBJECT_ID('"+procedureSam+"', 'P') IS NOT NULL DROP PROCEDURE "+procedureSam+"\n" +
            "IF OBJECT_ID('"+triggerName+"', 'TR') IS NOT NULL DROP TRIGGER "+triggerName+"\n" +
            "IF OBJECT_ID('"+triggerNameEncr+"', 'TR') IS NOT NULL DROP TRIGGER "+triggerNameEncr+"\n",

            // (6) procedure ddl
            "CREATE PROCEDURE "+procedureSam+" @word varchar(100) AS\n" +
            "BEGIN\n" +
            "    SELECT *\n" +
            "    FROM "+functionSamTable+"(@word) t\n" +
            "END",

            // (7) trigger ddl
            "CREATE TRIGGER "+triggerSam+"\n" +
            "ON "+sam+"\n" +
            "AFTER DELETE\n" +
            "NOT FOR REPLICATION\n" +
            "AS INSERT INTO "+sam+" SELECT * FROM deleted",

            // (8) trigger encrypted ddl
            "CREATE TRIGGER "+triggerSamEncrypted+"\n" +
            "ON "+sam+"\n" +
            "WITH ENCRYPTION\n" +
            "AFTER DELETE\n" +
            "AS INSERT INTO "+sam+" SELECT * FROM deleted"
        );

        for (String script : scripts){
            stmt.execute(script);
        }
        stmt.close();
        testConnection.commit();
        return scripts;
    }

    private void dropTestTriggerProcedureFunctions(String tableName) throws Exception{
        String schema = testConnection.getSchema();
        String sam = schema+"."+tableName;
        String functionSam = schema + "." + functionName;
        String functionSamTable = functionSam + "Table";
        String functionSamScalar = functionSam;
        String procedureSam = MessageFormat.format("{0}.{1}", schema, procedureName);

        Statement stmt = testConnection.createStatement();
        stmt.execute(
            MessageFormat.format(
                "DROP TABLE {0}\nDROP FUNCTION {1}, {2}\nDROP PROCEDURE {3}\n",
                sam, functionSamScalar, functionSamTable, procedureSam
            )
        );
        stmt.close();
    }
}

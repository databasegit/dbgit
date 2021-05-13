package ru.fusionsoft.dbgit;

import com.diogonunes.jcdp.color.api.Ansi;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.command.*;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;


@Tag("deprecated")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DBGitTest {

    static Path RESOURCES_REPO_DIR = new File("src/test/resources/repo").toPath();
    static Path RECOURCES_REPO_GIT_DIR = RESOURCES_REPO_DIR.resolve(".git");

    static String REPO_URL = "https://github.com/rocket-3/dbgit-test.git";
    static String REPO_BRANCH = "master";
    static List<String> REPO_COMMIT_NUMBERS = new ArrayList<>();

    static String TEST_DB_URL = "jdbc:postgresql://localhost/";
    static String TEST_DB_USER = "postgres";
    static String TEST_DB_PASS = "";
    static String TEST_DB_CATALOG = "test#databasegit";
    static boolean TO_CREATE_CATALOG = true;
    static int messageLevel = 0;
    
    private static void addCatalogToUrl() {
        TEST_DB_URL = MessageFormat.format(
            "{0}{1}{2}",
            TEST_DB_URL,
            TEST_DB_URL.endsWith("/") ? "" : "/",
            TEST_DB_CATALOG
        );
    }

    @BeforeAll
    public static void setUp() throws Exception {
        printMethodHead("Set up", null);
        ConsoleWriter.setDetailedLog(true);

        FileUtils.cleanDirectory(RESOURCES_REPO_DIR.toFile());
        DBGit.initUrlInstance(RECOURCES_REPO_GIT_DIR.toString(), false);
        dbgitClone(REPO_URL, String.valueOf(RESOURCES_REPO_DIR));

        configureTestDb(TO_CREATE_CATALOG);

        if(REPO_COMMIT_NUMBERS.isEmpty()){
            loadCommitNumbersFromRepo();
        }

    }

    @BeforeEach
    public void setUpEach() throws Exception {
//        printMethodHead("Before each", null);
        configureDBConnection();
    }

    @Test
    public void CmdLink() throws Exception {
        printMethodHead("CmdLink", null);

        CmdLink cmd = new CmdLink();
        String notValidUrl = "jdbc:postgresql://4.4.4.4/";
        File dblinkFile = RESOURCES_REPO_DIR.resolve(".dbgit").resolve(".dblink").toFile();
        dblinkFile.delete();

//        DriverManager.setLoginTimeout(3);
//        cmd.execute(getLinkCommandLine(notValidUrl, null, null));
//        assertFalse(dblinkFile.exists(), "'.dblink file not created on non-valid url'");

        cmd.execute(getLinkCommandLine(TEST_DB_URL, TEST_DB_USER, TEST_DB_PASS));
        assertTrue(dblinkFile.exists());

    }

    @Test
    public void CmdCheckout() throws Exception {

        printMethodHead("CmdCheckout", "init");
        boolean isNoDb          = true ;
        boolean isRestore       = false;
        boolean isCreateBranch  = false;
        boolean isUpgrade       = false;
        boolean isNoOwner       = true;

        String scriptPath = RESOURCES_REPO_DIR.resolve("checkout-r.sql").toAbsolutePath().toString();
        File scriptFile = new File(scriptPath);
        scriptFile.delete();
        GitMetaDataManager.getInstance().loadDBMetaData();

        printMethodHead("CmdCheckout", "just checkout to first commit (-nodb)");
        dbgitCheckout(REPO_BRANCH, REPO_COMMIT_NUMBERS.get(0), isNoDb, isRestore, isCreateBranch, isUpgrade, isNoOwner);

        printMethodHead("CmdCheckout", "try with restore to first commit (-r)");
        dbgitCheckout(REPO_BRANCH, REPO_COMMIT_NUMBERS.get(0), false, true, isCreateBranch, isUpgrade, isNoOwner);

        printMethodHead("CmdCheckout", "try with restore to last commit (-s ...)" );
        assertDoesNotThrow( () ->
            dbgitCheckout(
                    REPO_BRANCH, REPO_COMMIT_NUMBERS.get(REPO_COMMIT_NUMBERS.size() - 1), scriptPath,
                false, false, isCreateBranch, isUpgrade, !TO_CREATE_CATALOG
            )
        );
        assertTrue(scriptFile.exists());
        assertTrue(FileUtils.readFileToString(scriptFile).length() > 100);
    }

    @Test
    public void CmdRestore() throws Exception {

        boolean isRestore       = false;
        boolean isToMakeBackup  = true;
        boolean isNoDb          = true;
        boolean isCreateBranch  = false;
        boolean isUpgrade       = false;


        dbgitReset("hard");
        setToMakeBackup(isToMakeBackup);

        for(String commitNumber : REPO_COMMIT_NUMBERS){
            printMethodHead("CmdRestore", String.valueOf(REPO_COMMIT_NUMBERS.indexOf(commitNumber)) );

            String scriptPath = RESOURCES_REPO_DIR.resolve(MessageFormat.format(
                "restore#{0}.sql",
                REPO_COMMIT_NUMBERS.indexOf(commitNumber)
            )).toAbsolutePath().toString();
            String scriptPathA = scriptPath+"-again";
            File scriptFile = new File(scriptPath);
            File scriptFileA = new File(scriptPathA);

            scriptFile.delete();

            dbgitCheckout(REPO_BRANCH, commitNumber, scriptPath, isNoDb, isRestore, isCreateBranch, isUpgrade, !TO_CREATE_CATALOG);
            ConsoleWriter.printlnGreen(MessageFormat.format(
                    "\tIndex of commit in list: [{0}]", REPO_COMMIT_NUMBERS.indexOf(commitNumber)
                ), messageLevel
            );


            scriptFile.delete();
            dbgitRestore(true, true, scriptPath);
            scriptFileA.delete();
            dbgitRestore(false, true, scriptPathA);


            ConsoleWriter.printLineBreak();
            //TODO assert sizes have no difference
            ConsoleWriter.printlnGreen(MessageFormat.format(
                "File 1st: {0} syms.\nFile 2nd: {1} syms.",
                scriptFile.exists() ? FileUtils.readFileToString(scriptFile).length() : -1,
                scriptFileA.exists() ? FileUtils.readFileToString(scriptFileA).length() :- 1
            ), messageLevel);
        }
    }

    @Test
    public void CmdAdd() throws Exception {
        printMethodHead("CmdAdd", null);

        CmdAdd cmd = new CmdAdd();
        // checkout to last commit - restore files and database
        // checkout 1st commit - restore files, index
        // call cmdAdd
        // ensure files added (what files?) - from last commit db state

        String firstCommit = REPO_COMMIT_NUMBERS.get(0);
        String lastCommit = REPO_COMMIT_NUMBERS.get(REPO_COMMIT_NUMBERS.size()-1);

        dbgitCheckout(REPO_BRANCH, lastCommit, false, true, false, false, false);
        dbgitCheckout(REPO_BRANCH, firstCommit, true, false, false, false, false);

        IMapMetaObject fileImos = GitMetaDataManager.getInstance().loadFileMetaData();
        IMapMetaObject databaseImos = GitMetaDataManager.getInstance().loadDBMetaData();


        ConsoleWriter.detailsPrintln("Find file to db object difference: ", messageLevel);
        MapDifference<String, IMetaObject> diffs = Maps.difference(fileImos, databaseImos);

        diffs.entriesDiffering().forEach( (metaKey, metaObject) -> {
            ConsoleWriter.detailsPrintlnGreen(metaKey, messageLevel+1);

            Map<String, Map> fileImoMap = metaObject.leftValue().toMap();
            Map<String, Map> dbImoMap = metaObject.rightValue().toMap();
            printMapsDifference(dbImoMap, fileImoMap, messageLevel+1);

        });

    }

    //TODO вынести в отдельный метод с параметрами
    //@Test
    public void CmdClone() throws Exception {
        printMethodHead("CmdClone", null);

        FileUtils.cleanDirectory(RESOURCES_REPO_DIR.toFile());
        //TODO кстати, вот он
        dbgitClone(REPO_URL, String.valueOf(RESOURCES_REPO_DIR));
        configureDBConnection();


        File gitFile = RESOURCES_REPO_DIR.resolve(".git").toFile();
        File dbgitFile = RESOURCES_REPO_DIR.resolve(".dbgit").toFile();
        File[] dbgitFiles = dbgitFile.listFiles();

        assertTrue(gitFile.exists(), "'.git folder exists'");
        assertTrue(dbgitFile.exists(), "'.dbgit folder exists'");
        assertTrue(dbgitFiles != null && dbgitFiles.length > 0, "'.dbgit folder is not empty'");

        ConsoleWriter.printlnGreen(MessageFormat.format("Number of files in .dbgit {0}", dbgitFiles.length ), messageLevel);
        Arrays.asList(dbgitFiles).forEach(x->ConsoleWriter.printlnRed(x.getPath(), messageLevel+1));

    }

    //@Test
    public void CmdReset() throws Exception {
        printMethodHead("CmdReset", null);

        dbgitCheckout(REPO_BRANCH, REPO_COMMIT_NUMBERS.get(0), true, false, false, false, false);

        File dblinkFile = RESOURCES_REPO_DIR.resolve(".dbgit").resolve(".dblink").toFile();
        List<String> modes = Arrays.asList("soft", "mixed", "hard", "merge", "keep");

        dblinkFile.delete();
        assertFalse(dblinkFile.exists());

        dbgitReset("hard");
        assertTrue(dblinkFile.exists());

    }

    private static void dbgitClone(String urlFrom, String dirTo) throws Exception {
        CmdClone cmdClone = new CmdClone();
        StringBuilder sb = new StringBuilder();

        Option directoryOption = Option.builder().longOpt("directory").hasArg(true).numberOfArgs(1).build();
        directoryOption.getValuesList().add(dirTo);

        CommandLine cmdLineClone = new CommandLine.Builder()
            .addArg(urlFrom)
            .addOption(directoryOption).build();

        sb.append(urlFrom + " ");
        sb.append("-directory " + dirTo);

        ConsoleWriter.printlnGreen(MessageFormat.format("(call) dbgit {0} {1}", "clone", sb.toString()), messageLevel);

        cmdClone.execute(cmdLineClone);
    }

    private static void dbgitReset(String mode) throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(mode);

        CmdReset cmd = new CmdReset();
        Option optionMode = new Option(mode , false, mode);
        CommandLine.Builder builder = new CommandLine.Builder();
        ConsoleWriter.println(MessageFormat.format("(call) dbgit {0} {1}", "reset", sb.toString()), messageLevel);

        cmd.execute(builder.addOption(optionMode).build());
    }

    private static void dbgitCheckout(String branchName, String commitNumber,
          boolean isNoDb, boolean isRestore, boolean isCreateBranch,
          boolean isUpgrade, boolean isNoOwner) throws Exception {
        dbgitCheckout(branchName, commitNumber, null,
            isNoDb, isRestore, isCreateBranch,
            isUpgrade, isNoOwner
        );
    }
    private static void dbgitCheckout(String branchName, String commitNumber, String scriptPath,
          boolean isNoDb, boolean isRestore, boolean isCreateBranch,
          boolean isUpgrade, boolean isNoOwner
    ) throws Exception {
        CmdCheckout cmd = new CmdCheckout();
        CommandLine.Builder builder = new CommandLine.Builder();
        StringBuilder sb = new StringBuilder();

        String actualBranchName =  branchName == null ? "master" : branchName;

        Option nodbOption = Option.builder("nodb").hasArg(false).build();
        Option noOwnerOption = Option.builder("noowner").hasArg(false).build();
        Option restoreOption = Option.builder("r").hasArg(false).build();
        Option newBranchOption = Option.builder("b").hasArg(false).build();
        Option upgradeOption = Option.builder("u").hasArg(false).build();
        Option scriptOption = Option.builder("s").hasArg(true).build();

        builder.addArg(actualBranchName);
        if(commitNumber != null){
            builder.addArg(commitNumber);
            sb.append(commitNumber + " ");
        }
        if(isNoDb){
            builder.addOption(nodbOption);
            sb.append("-nodb ");
        }
        if(isRestore){
            builder.addOption(restoreOption);
            sb.append("-r ");
        }
        if(isCreateBranch){
            builder.addOption(newBranchOption);
            sb.append("-b ");
        }
        if(isUpgrade){
            builder.addOption(upgradeOption);
            sb.append("-u ");
        }
        if(isNoOwner){
            builder.addOption(noOwnerOption);
            sb.append("-noowner ");

        }
        if(scriptPath != null){
            scriptOption.getValuesList().add(scriptPath);
            builder.addOption(scriptOption);
            sb.append("-s ").append(scriptPath).append(" ");
        }
        builder.addOption(Option.builder("v").hasArg(false).build());
        ConsoleWriter.println(MessageFormat.format("(call) dbgit {0} {1}", "checkout", sb.toString()), messageLevel);


        cmd.execute(builder.build());
    }

    private static void dbgitCheckoutLs() throws Exception {
        CmdCheckout cmd = new CmdCheckout();
        CommandLine.Builder builder = new CommandLine.Builder();

        Option lsOption = Option.builder("ls").hasArg(false).build();
        builder.addOption(lsOption);

        ConsoleWriter.println(MessageFormat.format("(call) dbgit {0}", "checkout -ls"), messageLevel);
        cmd.execute(builder.build());
    }

    private static void dbgitRestore(boolean isRestore, boolean isToMakeBackup, String scriptPath) throws Exception {

        CmdRestore cmd = new CmdRestore();
        CommandLine.Builder builder = new CommandLine.Builder();
        StringBuilder sb = new StringBuilder();

        builder.addOption(Option.builder("v").hasArg(false).build());
        sb.append("-v ");

        if(isRestore){
            Option restoreOption = Option.builder("r").hasArg(false).build();
            builder.addOption(restoreOption);
            sb.append("-r ");
        }
        if(!TO_CREATE_CATALOG){
            Option noownerOption = Option.builder("noowner").hasArg(false).build();
            builder.addOption(noownerOption);
            sb.append("-noowner ");
        }
        if(scriptPath != null){
            Option scriptOption = Option.builder("s").hasArg(true).numberOfArgs(1).build();
            scriptOption.getValuesList().add(scriptPath);
            builder.addOption(scriptOption);
            sb.append("-s ").append(scriptPath);
        }

        ConsoleWriter.println(MessageFormat.format("(call) dbgit {0} {1}", "checkout", sb.toString()), messageLevel);
        setToMakeBackup(isToMakeBackup);
        cmd.execute(builder.build());
    }

    private static void configureTestDb(boolean tryCreateCatalog) throws Exception {
        Properties testDbProps = new Properties();

        String propDbUrl = System.getProperty("pgTestDbUrl");
        String propDbUser = System.getProperty("pgTestDbUser");
        String propDbPass = System.getProperty("pgTestDbPass");

        ConsoleWriter.println("(config) Using test database: ", messageLevel);
        if(propDbUrl != null){
            ConsoleWriter.print("<from command line> ");
            TEST_DB_URL = propDbUrl;
            TEST_DB_USER = propDbUser;
            TEST_DB_PASS = propDbPass;
        } else {
            ConsoleWriter.print("<hardcoded> ");
        }
        ConsoleWriter.print(TEST_DB_URL);

        if(TEST_DB_USER != null && TEST_DB_PASS != null){
            testDbProps.put("user", TEST_DB_USER);
            testDbProps.put("password", TEST_DB_PASS);
        }

        if(tryCreateCatalog){
            try (Connection conn = DriverManager.getConnection(TEST_DB_URL, testDbProps)) {

                if (!conn.getCatalog().isEmpty()) {
                    throw new Exception("Catalog must not be specified to create test catalog.");
                }

                IDBAdapter adapter = AdapterFactory.createAdapter(conn);

                try(Statement stmt = conn.createStatement()){
                    stmt.execute(MessageFormat.format(
                        "DROP DATABASE \"{0}\"'; ",
                        adapter.escapeNameIfNeeded(TEST_DB_CATALOG)
                    ));
                } catch (Exception ex){
                    ConsoleWriter.println(
                    "(config) failed to drop database: " + ex.getMessage().replaceAll("\n", ";")
                        , messageLevel
                    );
                }

                try(Statement stmt = conn.createStatement()){
                    stmt.execute(MessageFormat.format(
                        "CREATE DATABASE \"{0}\"; ",
                        adapter.escapeNameIfNeeded(TEST_DB_CATALOG)
                    ));
                } catch (Exception ex){
                    ConsoleWriter.println(
                        "(config) failed to create database: " + ex.getMessage().replaceAll("\n", ";")
                        , messageLevel
                    );
                    //throw  ex;
                }

            }
        }
        addCatalogToUrl();


        DBConnection.createFileDBLink(TEST_DB_URL, testDbProps, false);

    }

    private static void configureDBConnection() throws Exception {
        String dbLinkUrl = DBConnection.loadFileDBLink(new Properties());

        if( !DBConnection.hasInstance() ||
            !DBConnection.getInstance().getConnect().getMetaData().getURL().equals(TEST_DB_URL)){

            new CmdLink().execute(getLinkCommandLine(TEST_DB_URL, TEST_DB_USER, TEST_DB_PASS));
            if(DBConnection.hasInstance()) {
                DBConnection.getInstance().flushConnection();
                DBConnection.getInstance();
            }

            ConsoleWriter.printlnGreen(MessageFormat.format(
                "(config) Set up DBConnection and ''.dblink'' state, \n\twas\t: {0} \n\tset\t: {1}"
                , dbLinkUrl
                , DBConnection.getInstance().getConnect().getMetaData().getURL()
            ), messageLevel);
        }

        //not to broke any other database
        assertEquals(TEST_DB_URL, DBConnection.getInstance().getConnect().getMetaData().getURL(), "DBConnection url IS NOT equal to test database url" );
    }

    private static void loadCommitNumbersFromRepo() throws GitAPIException, IOException {
        InMemoryRepository repo = new InMemoryRepository(new DfsRepositoryDescription());
        Git git = new Git(repo);
        git.fetch()
            .setRemote(REPO_URL)
            .setRefSpecs(new RefSpec("+refs/heads/"+ REPO_BRANCH +":refs/heads/"+ REPO_BRANCH))
            .call();

        String treeName = "refs/heads/"+ REPO_BRANCH; // tag or branch
        for (RevCommit commit :  git.log().add(repo.resolve(treeName)).call()) {
            REPO_COMMIT_NUMBERS.add(commit.getName());
        }
    }

    private static void setToMakeBackup(boolean isToMakeBackup) throws Exception {
        String sectionName = "core";
        String parameterName = "TO_MAKE_BACKUP";
        String was = String.valueOf(DBGitConfig.getInstance().getBoolean(sectionName, parameterName, false));

        DBGitConfig.getInstance().setValue("TO_MAKE_BACKUP", isToMakeBackup ? "true" : "false");

        ConsoleWriter.detailsPrintlnRed(MessageFormat.format("(config) Set ''TO_MAKE_BACKUP'' (was, set, now): {0}, {1}, {2}",
                was, String.valueOf(isToMakeBackup),
                String.valueOf(DBGitConfig.getInstance().getBoolean(sectionName, parameterName, false))
        ), messageLevel);
    }

    private static CommandLine getLinkCommandLine(String url, String user, String pass){
        CommandLine.Builder builder = new CommandLine.Builder().addArg(url);
        if(user != null) {
            builder.addArg("user=" + user);

        }
        if(pass != null) {
            builder.addArg("password=" + pass);
        }
        return builder.build();
    }

    private static void printMethodHead(String name, String part){
        String text = name + ((part != null) ? " part '" + part + "'" : "") ;
        int length = 92;
        int textLength = text.length();
        boolean shouldAddSpace = textLength % 2 != length % 2;
        int remSpaceLength = length - textLength - (shouldAddSpace ? 0 : 1);
        int sideHeaderLength = remSpaceLength / 2;
        StringBuilder sb = new StringBuilder();

        for ( int step = 0; step < 2; step ++){
            if( step == 0 ) sb.append(text);
            for (int i = sideHeaderLength; i > 0;){
                sb.append(" -"); i -= 2;
                if( i == 2 ) { sb.append(" "); i--; }
                if( i == 1 ) { sb.append("|\n") ; i--; }
            }

            sb.reverse();
        }

        ConsoleWriter.printlnColor(sb.toString(), Ansi.FColor.MAGENTA, 0);
    }

    private static void printMapsDifference(Map mapBefore, Map mapAfter, int level){

        MapDifference difference = Maps.difference(mapBefore, mapAfter);
        Map<String, Object> add = difference.entriesOnlyOnRight();
        Map<String, Object> remove = difference.entriesOnlyOnLeft();
        Map<String, MapDifference.ValueDifference<Object>> change = difference.entriesDiffering();

        for (Map.Entry<String, Object> addEntry : add.entrySet()) {
            ConsoleWriter.println(MessageFormat.format("+ ''{0}'': ", addEntry.getKey()), level);
            printNode( addEntry.getValue(), level );
        }

        for (Map.Entry<String, Object> removeEntry : remove.entrySet()) {
            ConsoleWriter.println(MessageFormat.format("- ''{0}'': ", removeEntry.getKey()), level);
            //printNode( removeEntry.getValue(), level );
        }

        for (Map.Entry<String, MapDifference.ValueDifference<Object>> changeEntry : change.entrySet()) {
            ConsoleWriter.println(MessageFormat.format("* ''{0}'': ", changeEntry.getKey()), level);

            Object valueBefore = changeEntry.getValue().leftValue();
            Object valueAfter = changeEntry.getValue().rightValue();

            if( valueBefore instanceof Map && valueAfter instanceof Map ){
                printMapsDifference((Map) valueBefore, (Map) valueAfter, level+1);
            } else {
                ConsoleWriter.print(MessageFormat.format("\"{0}\" -> \"{1}\"", valueBefore, valueAfter));
            }

        }
    }

    private static void printNode(Object node, int level){
        if(node instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) node;

            for (Map.Entry entry : map.entrySet()) {
                ConsoleWriter.println(MessageFormat.format("''{0}'': " ,entry.getKey()), level+1);
                printNode( entry.getValue(), level+1);
            }
        }
        else {
            ConsoleWriter.print(node != null ? node : "");
        }
    }
}

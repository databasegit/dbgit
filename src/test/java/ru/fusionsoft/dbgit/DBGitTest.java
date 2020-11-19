package ru.fusionsoft.dbgit;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.dfs.DfsRepositoryDescription;
import org.eclipse.jgit.internal.storage.dfs.InMemoryRepository;
import org.eclipse.jgit.internal.storage.file.FileRepository;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.RefSpec;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;

import org.postgresql.jdbc.PgConnection;
import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.command.*;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.postgres.DBAdapterPostgres;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;


// 1. We use test repository in 'resources' folder
// 2. We use test database created in selected server
// 3. We have various test scenarios to test COMMANDS and their COMBINATIONS
// + CmdAdd - check add files from DATABASE to REPO
//CmdRm - check delete files, DBINDEX operations, CmdRestore interchange,
//CmdDump - check the same as CmdAdd
//CmdBackup - check add backup files to test DATABASE
// +- CmdRestore - lots of scenarios on 1. restore schema 2. delete objs 3. modify schema 4. modify data 5. backups
// + CmdCheckout
//CmdStatus CmdValid
// + CmdLink CmdInit CmdConfig
//CmdCommit CmdPush CmdPull CmdFetch
//CmdRemote CmdMerge CmdReset
//CmdSynonymSchema CmdHelp CmdClone
//


@Tag("pgTest")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class DBGitTest {

    static String repoUrl = "https://github.com/rocket-3/dbgit-test.git";
    static String repoBranch = "master";
    static String pgTestDbUrl = "jdbc:postgresql://0.0.0.0/";
    static String pgTestDbUser = /*"postgres";*/"testuser";
    static String pgTestDbPass = /*"pass";*/"s%G351as";
    static String pgTestDbCatalog = "testdatabasegit";
    static boolean eraseExistingCatalog = false;
    static{ if(!eraseExistingCatalog) addCatalogToUrl(); }

    private static void addCatalogToUrl() {
        pgTestDbUrl = MessageFormat.format(
            "{0}{1}{2}",
            pgTestDbUrl,
            !pgTestDbUrl.endsWith("/") ? "/" : "",
            pgTestDbCatalog
        );
    }

    static List<String> commitNumbers = new ArrayList<>();
    //Now commit numbers are loaded automaticvally
    /*Arrays.asList(
        "88673845e9891228d2ebb7ad3bdebb534ce8efbe",
        "b1fecd7ebcf33a61fa7b962cb5e92ae6cdb8db64",
        "831054e4eac9a1f0d1797b6f65ebd64e7d81f74f",
        "d2b40808dd6df2d51d80bddbdcdfdabef7394373",
        "4f3953d44743a539321d024871182eef8f1cd7f9"
    );*/
    static Path resourcesRepoDirectory = new File( "src/test/resources/repo").toPath();
    static Path resourcesRepoGitDirectory = resourcesRepoDirectory.resolve(".git");

    static Properties pgTestDbProps = new Properties();
    static DBConnection pgTestDbConnection = null;



    @BeforeAll
    public static void setUp() throws Exception {
        DBGit.initUrlInstance(resourcesRepoGitDirectory.toString(), false);

        if(commitNumbers.isEmpty()){
            loadCommitNumbersFromRepo();
        }

        configureTestDb(false);
    }

    @BeforeEach
    public void init() throws Exception {
        restoreDbLinkIfNeeded();
    }

    @Test
    @Order(1)
    public void CmdClone() throws Exception {
        FileUtils.cleanDirectory(resourcesRepoDirectory.toFile());
        dbgitClone(repoUrl, String.valueOf(resourcesRepoDirectory));


        File gitFile = resourcesRepoDirectory.resolve(".git").toFile();
        File dbgitFile = resourcesRepoDirectory.resolve(".dbgit").toFile();
        File[] dbgitFiles = dbgitFile.listFiles();

        assertTrue(gitFile.exists(), "'.git folder exists'");
        assertTrue(dbgitFile.exists(), "'.dbgit folder exists'");
        assertTrue(dbgitFiles != null && dbgitFiles.length > 0, "'.dbgit folder is not empty'");

        ConsoleWriter.printlnGreen(MessageFormat.format("Number of files in .dbgit {0}", dbgitFiles.length ));
        Arrays.asList(dbgitFiles).forEach(x->ConsoleWriter.printlnRed(x.getPath()));

    }

    @Test
    @Order(2)
    public void CmdLink() throws Exception {
        CmdLink cmd = new CmdLink();
        String notValidUrl = "jdbc:postgresql://4.4.4.4/";
        File dblinkFile = resourcesRepoDirectory.resolve(".dbgit").resolve(".dblink").toFile();
        dblinkFile.delete();

//        DriverManager.setLoginTimeout(3);
//        cmd.execute(getLinkCommandLine(notValidUrl, null, null));
//        assertFalse(dblinkFile.exists(), "'.dblink file not created on non-valid url'");

        cmd.execute(getLinkCommandLine(pgTestDbUrl, pgTestDbUser, pgTestDbPass));
        assertTrue(dblinkFile.exists());

    }

    @Test
    @Order(3)
    public void CmdCheckout() throws Exception {
        boolean isNoDb          = true;
        boolean isRestore       = false;
        boolean isCreateBranch  = false;
        boolean isUpgrade       = false;

        GitMetaDataManager.getInstance().loadDBMetaData();
        for(String commitNumber : commitNumbers){
            dbgitCheckout(repoBranch, commitNumber, isNoDb, isRestore, isCreateBranch, isUpgrade);
        }

        assertDoesNotThrow( () ->dbgitCheckout(repoBranch, commitNumbers.get(0), false, true, isCreateBranch, isUpgrade) );
    }

    @Test
    @Order(4)
    public void CmdReset() throws Exception {

        File dblinkFile = resourcesRepoDirectory.resolve(".dbgit").resolve(".dblink").toFile();
        List<String> modes = Arrays.asList("soft", "mixed", "hard", "merge", "keep");

        dblinkFile.delete();
        assertFalse(dblinkFile.exists());

        dbgitReset("hard");
        assertTrue(dblinkFile.exists());


    }

    @Test
    @Order(5)
    public void CmdRestore() throws Exception {
        boolean isRestore       = false;
        boolean isToMakeBackup  = true;
        boolean isNoDb          = true;
        boolean isCreateBranch  = false;
        boolean isUpgrade       = false;


        dbgitReset("hard");
        setToMakeBackup(isToMakeBackup);

        for(String commitNumber : commitNumbers){
            String  scriptPath = resourcesRepoDirectory.resolve("restore#"+commitNumber+".sql").toAbsolutePath().toString();
            File    scriptFile = new File(scriptPath);

            scriptFile.delete();
            ConsoleWriter.printlnGreen("\nDoing checkout:");

            dbgitCheckout(repoBranch, commitNumber, isNoDb, isRestore, isCreateBranch, isUpgrade, scriptPath);
            dbgitCheckoutLs();

//            ConsoleWriter.printlnGreen(MessageFormat.format("++ checkout: {0} symbols, path:\n{1}",
//                 scriptFile.exists() ? FileUtils.readFileToString(scriptFile).length() : 0,
//                 scriptPath
//            ));

            scriptFile.delete();
            dbgitRestore(true, true, scriptPath);

            ConsoleWriter.printlnGreen(MessageFormat.format("Done restore, script: \n{1} ({0} syms.)",
                scriptFile.exists() ? FileUtils.readFileToString(scriptFile).length() : 0,
                scriptPath
            ));
        }
    }

    @Test
    @Order(6)
    public void CmdAdd() throws Exception {
        CmdAdd cmd = new CmdAdd();
        // checkout to last commit - restore files and database
        // checkout 1st commit - restore files, index
        // call cmdAdd
        // ensure files added (what files?) - from last commit db state

        String firstCommit = commitNumbers.get(0);
        String lastCommit = commitNumbers.get(commitNumbers.size()-1);

        dbgitCheckout(repoBranch, lastCommit, false, true, false, false);
        IMapMetaObject fileImos = GitMetaDataManager.getInstance().loadFileMetaData();
        IMapMetaObject databaseImos = GitMetaDataManager.getInstance().loadDBMetaData();
        MapDifference<String, IMetaObject> diffs = Maps.difference(fileImos, databaseImos);

        ConsoleWriter.detailsPrintLn("Diffs: ");
        diffs.entriesDiffering().forEach( (key, value) -> {
           ConsoleWriter.detailsPrintLn(MessageFormat.format("{0} -> ({1}, {2})",
               key, value.leftValue(), value.rightValue()));
        });


    }


    private static void dbgitClone(String urlFrom, String dirTo) throws Exception {
        CmdClone cmdClone = new CmdClone();
        Option directoryOption = Option.builder().longOpt("directory").hasArg(true).numberOfArgs(1).build();
        directoryOption.getValuesList().add(dirTo);
        CommandLine cmdLineClone = new CommandLine.Builder().addArg(urlFrom).addOption(directoryOption).build();
        cmdClone.execute(cmdLineClone);
    }

    private static void dbgitReset(String mode) throws Exception {
        CmdReset cmd = new CmdReset();
        Option optionMode = new Option(mode , false, mode);
        CommandLine.Builder builder = new CommandLine.Builder();
        cmd.execute(builder.addOption(optionMode).build());
    }

    private static void dbgitCheckout(String branchName, String commitNumber, boolean isNoDb, boolean isRestore, boolean isCreateBranch, boolean isUpgrade) throws Exception {
        dbgitCheckout(branchName, commitNumber, isNoDb, isRestore, isCreateBranch, isUpgrade, null);
    }
    private static void dbgitCheckout(String branchName, String commitNumber, boolean isNoDb, boolean isRestore, boolean isCreateBranch, boolean isUpgrade, String scriptPath
    ) throws Exception {
        CmdCheckout cmd = new CmdCheckout();
        CommandLine.Builder builder = new CommandLine.Builder();
        String actualBranchName =  branchName == null ? "master" : branchName;

        Option nodbOption = Option.builder("nodb").hasArg(false).build();
        Option restoreOption = Option.builder("r").hasArg(false).build();
        Option newBranchOption = Option.builder("b").hasArg(false).build();
        Option upgradeOption = Option.builder("u").hasArg(false).build();
        Option scriptOption = Option.builder("s").hasArg(true).build();

        builder.addArg(actualBranchName);
        if(commitNumber != null){
            builder.addArg(commitNumber);
        }
        if(isNoDb){
            builder.addOption(nodbOption);
        }
        if(isRestore){
            builder.addOption(restoreOption);
        }
        if(isCreateBranch){
            builder.addOption(newBranchOption);
        }
        if(isUpgrade){
            builder.addOption(upgradeOption);
        }
        if(scriptPath != null){
            scriptOption.getValuesList().add(scriptPath);
            builder.addOption(scriptOption);
        }

        cmd.execute(builder.build());
    }

    private static void dbgitCheckoutLs() throws Exception {
        CmdCheckout cmd = new CmdCheckout();
        CommandLine.Builder builder = new CommandLine.Builder();

        Option lsOption = Option.builder("ls").hasArg(false).build();
        builder.addOption(lsOption);

        cmd.execute(builder.build());
    }

    private static void dbgitRestore(boolean isRestore, boolean isToMakeBackup, String scriptPath) throws Exception {
        CmdRestore cmd = new CmdRestore();
        CommandLine.Builder builder = new CommandLine.Builder();


        if(isRestore){
            Option restoreOption = Option.builder("r").hasArg(false).build();
            builder.addOption(restoreOption);
        }
        if(scriptPath != null){
            Option scriptOption = Option.builder("s").hasArg(true).numberOfArgs(1).build();
            scriptOption.getValuesList().add(scriptPath);
            builder.addOption(scriptOption);
        }
        setToMakeBackup(isToMakeBackup);

        cmd.execute(builder.build());
    }

    private static void configureTestDb(boolean eraseDatabase) throws Exception {
        String propDbUrl = System.getProperty("pgTestDbUrl");
        String propDbUser = System.getProperty("pgTestDbUser");
        String propDbPass = System.getProperty("pgTestDbPass");
        if(propDbUrl != null){
            ConsoleWriter.printlnGreen(MessageFormat.format("Overriding DBConnection url from props: {0}", pgTestDbUrl));
            pgTestDbUrl = propDbUrl;
            pgTestDbUser = propDbUser;
            pgTestDbPass = propDbPass;
        } else {
            ConsoleWriter.printlnGreen(MessageFormat.format("Using defaults DBConnection url: {0}", pgTestDbUrl));
        }
        if(pgTestDbUser != null && pgTestDbPass != null){
            pgTestDbProps.put("user", pgTestDbUser);
            pgTestDbProps.put("password", pgTestDbPass);
        }
        try (Connection conn = DriverManager.getConnection(pgTestDbUrl, pgTestDbProps)) {

            if(eraseDatabase){
                if (!conn.getCatalog().isEmpty()) {
                    throw new Exception("Catalog must not be specified to create test database.");
                }

                IDBAdapter adapter = AdapterFactory.createAdapter();

                try(Statement stmt = conn.createStatement()){
                    stmt.execute(MessageFormat.format(
                        "DROP DATABASE {0}; ",
                        AdapterFactory.createAdapter().escapeNameIfNeeded(pgTestDbCatalog)
                    ));
                } catch (Exception ex){ ConsoleWriter.println("### failed to drop database: " + ex.getLocalizedMessage()); }

                try(Statement stmt = conn.createStatement()){
                    stmt.execute(MessageFormat.format(
                        "CREATE DATABASE {0} ENCODING = 'UTF8'",
                        adapter.escapeNameIfNeeded(pgTestDbCatalog)
                    ));
                } catch (Exception ex){
                    ConsoleWriter.println("### failed to create database: " + ex.getLocalizedMessage());
                    throw  ex;
                }

                addCatalogToUrl();
            }

        }

        DBConnection.createFileDBLink(pgTestDbUrl, pgTestDbProps, false);
        pgTestDbConnection = DBConnection.getInstance(true);

    }

    private static void loadCommitNumbersFromRepo() throws GitAPIException, IOException {
        DfsRepositoryDescription repoDesc = new DfsRepositoryDescription();
        InMemoryRepository repo = new InMemoryRepository(repoDesc);
        Git git = new Git(repo);
        git.fetch()
                .setRemote(repoUrl)
                .setRefSpecs(new RefSpec("+refs/heads/"+repoBranch+":refs/heads/"+repoBranch))
                .call();

        String treeName = "refs/heads/"+repoBranch; // tag or branch
        for (RevCommit commit :  git.log().add(repo.resolve(treeName)).call()) {
            commitNumbers.add(commit.getName());
        }
    }

    private static void restoreDbLinkIfNeeded() throws Exception {
        String urlWas = DBConnection.loadFileDBLink(new Properties());
        if(pgTestDbConnection == null || !urlWas.equals(pgTestDbUrl)){
            new CmdLink().execute(getLinkCommandLine(pgTestDbUrl, pgTestDbUser, pgTestDbPass));
            pgTestDbConnection = DBConnection.getInstance();

            ConsoleWriter.printlnGreen(MessageFormat.format(
                "+ .dblink restored, \nwas: {0}, \nnow: {1}"
                , urlWas, ((PgConnection) pgTestDbConnection.getConnect()).getURL()
            ));
        }
        assertEquals(((PgConnection) pgTestDbConnection.getConnect()).getURL(), pgTestDbUrl);
    }

    private static void setToMakeBackup(boolean isToMakeBackup) throws Exception {
        String sectionName = "core";
        String parameterName = "TO_MAKE_BACKUP";
        ConsoleWriter.detailsPrintlnRed(MessageFormat.format("+ TO_MAKE_BACKUP was: {0}",
                String.valueOf(DBGitConfig.getInstance().getBoolean(sectionName, parameterName, false))
        ));
        DBGitConfig.getInstance().setValue("TO_MAKE_BACKUP", isToMakeBackup ? "true" : "false");

        ConsoleWriter.detailsPrintlnRed(MessageFormat.format("+ TO_MAKE_BACKUP (set, now)): {0}, {1}",
                String.valueOf(isToMakeBackup),
                String.valueOf(DBGitConfig.getInstance().getBoolean(sectionName, parameterName, false))
        ));
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


}

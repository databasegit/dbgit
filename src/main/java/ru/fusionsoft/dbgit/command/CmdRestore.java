package ru.fusionsoft.dbgit.command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.diogonunes.jcdp.color.api.Ansi;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import org.cactoos.Scalar;
import org.cactoos.list.ListEnvelope;
import org.cactoos.scalar.ScalarOf;
import org.cactoos.scalar.Sticky;
import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

public class CmdRestore implements IDBGitCommand {
	private Options opts = new Options();

	public CmdRestore() {
		opts.addOption("s", true, getLang().getValue("help", "restore-s").toString());
		opts.addOption("r", false, getLang().getValue("help", "restore-r").toString());
	}

	public String getCommandName() {
		return "restore";
	}

	public String getParams() {
		return "";
	}

	public String getHelperInfo() {
		return getLang().getValue("help", "restore").toString();
	}

	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		IDBAdapter adapter = null;
		File autoScriptFile = createScriptFile();
		FileOutputStream scriptOutputStream = new FileOutputStream(autoScriptFile);
		GitMetaDataManager gmdm = GitMetaDataManager.getInstance();

		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
		ConsoleWriter.println(getLang().getValue("general", "restore", "do"), 0);

		boolean toMakeChanges = cmdLine.hasOption("r");
		boolean toMakeBackup = DBGitConfig.getInstance().getBoolean("core", "TO_MAKE_BACKUP", true);
		// from cmdLine to temporary config bypass
		// can be used as other config data by restore adapters
		DBGitConfig.getInstance().setToIgnoreOnwer(cmdLine.hasOption("noowner"));

		try {
			adapter = AdapterFactory.createAdapter();
			adapter.setDumpSqlCommand(scriptOutputStream, toMakeChanges);
		} catch (NullPointerException e) {
			ConsoleWriter.println(getLang().getValue("errors", "restore", "cantConnect"), messageLevel);
			System.exit(0);
		}

		IMapMetaObject dbObjs = gmdm.loadDBMetaData();
		IMapMetaObject fileObjs = gmdm.loadFileMetaData();
		IMapMetaObject updateObjs = new TreeMapMetaObject();
		IMapMetaObject deleteObjs = new TreeMapMetaObject();
		IMapMetaObject backupObjs = new TreeMapMetaObject();

		if (toMakeBackup) { ConsoleWriter.printlnColor(getLang().getValue("general", "restore", "willMakeBackup").toString(), Ansi.FColor.GREEN, messageLevel+1); } 
		else { ConsoleWriter.printlnColor(getLang().getValue("general", "restore", "wontMakeBackup").toString(), Ansi.FColor.GREEN, messageLevel+1); }
		if (toMakeChanges) { ConsoleWriter.printlnColor(getLang().getValue("general", "restore", "toMakeChanges").toString(), Ansi.FColor.GREEN, messageLevel+1); }
		else { ConsoleWriter.printlnColor(getLang().getValue("general", "restore", "notMakeChanges").withParams(autoScriptFile.getAbsolutePath()), Ansi.FColor.GREEN, messageLevel+1); }

		//delete that not present in HEAD
		try {
			DBGitIndex index = DBGitIndex.getInctance();
			DBGitIgnore ignore = DBGitIgnore.getInstance();

			ConsoleWriter.println(getLang().getValue("general", "restore", "seekingToRemove"),1);
//			ConsoleWriter.print(getLang().getValue("general", "restore", "toRemove"));

			for ( ItemIndex item : index.getTreeItems().values() ) {
				if ( ignore.matchOne(item.getName()) ) continue;

				if ( item.getIsDelete() ) {

					if ( !dbObjs.containsKey(item.getName()) ) {
						ConsoleWriter.println(getLang().getValue("general", "restore", "notExists").withParams(item.getName()), 2);
						index.removeItem(item.getName());
					} else {

						try {
							IMetaObject obj = MetaObjectFactory.createMetaObject(item.getName());
							gmdm.loadFromDB(obj);
							if (item.getHash().equals(obj.getHash())) {
								deleteObjs.put(obj);

								ConsoleWriter.println(getLang().getValue("general", "restore", "objectToRemove").withParams(obj.getName()), 2);
							}
						} catch(ExceptionDBGit e) {
							throw e;
							//LoggerUtil.getGlobalLogger().error(getLang().getValue("errors", "restore", "cantConnect") + ": " + item.getName(), e);
						}
					}
				}
			}
			if (deleteObjs.size() == 0) ConsoleWriter.println(getLang().getValue("general", "restore", "nothingToRemove").toString(), 2);


			ConsoleWriter.println(getLang().getValue("general", "restore", "seekingToRestore"),1);
			for (IMetaObject obj : fileObjs.values()) {

				//запомнили файл если хеш разный или объекта нет
				if (checkNeedsRestore(obj)) {
					updateObjs.put(obj);
//					if (updateObjs.size() == 1){
//						ConsoleWriter.print(getLang().getValue("general", "restore", "toRestore"));
//					}
					ConsoleWriter.println(obj.getName(), 2);
				}
			}
			if (updateObjs.size() == 0){
				ConsoleWriter.println(getLang().getValue("general", "restore", "nothingToRestore").toString(), 2);
			}

			// to fix pk constraint re-creation error
			// collect other file objects that depend on update objects
			// to re-create their fk constraints too, so we:

			// 0. enrich update list with fk-dependant objects from database
			// 1. drop all of constraints
			// 2. re-create all constraints in default sorted order

			// # steps 1,2 are in GitMetaDataManager::restoreDatabase

			ConsoleWriter.println(getLang().getValue("general", "restore", "seekingToRestoreAdditional"), messageLevel+2);
			Map<String, IMetaObject> updateObjectsCopy = new TreeMapMetaObject(updateObjs.values());
			Map<String, IMetaObject> affectedTables = new TreeMapMetaObject();
			Map<String, IMetaObject> foundTables = new TreeMapMetaObject();
			do {
				foundTables = 
					dbObjs.values().stream()
					.filter(excluded -> {
						return excluded instanceof MetaTable
						&& ! updateObjectsCopy.containsKey(excluded.getName())
						&& updateObjectsCopy.values().stream().anyMatch(excluded::dependsOn);
					})
					.collect(Collectors.toMap(IMetaObject::getName, val -> val));
				affectedTables.putAll(foundTables);
				updateObjectsCopy.putAll(foundTables);
				deleteObjs.putAll(foundTables);
				backupObjs.putAll(foundTables);
			} while (!foundTables.isEmpty());
			
			if(affectedTables.isEmpty()){
				ConsoleWriter.println(getLang().getValue("general", "restore", "nothingToRestoreAdditional"), messageLevel+2);
			} else {
				affectedTables.forEach((k,v)->ConsoleWriter.println(k, messageLevel+3));
			}

			//delete MetaSql (but no UDT's, domains or enums) that are in files and in db to fix errors on table restore
			ConsoleWriter.println(getLang().getValue("general", "restore", "droppingSqlObjects"), messageLevel+2);
			for (final IMetaObject object : 
				new SortedListMetaObject(
					dbObjs.entrySet().stream()
					.filter(x -> fileObjs.containsKey(x.getKey()))
					.map(Map.Entry::getValue)
					.filter(x -> 
						x instanceof MetaFunction || 
						x instanceof MetaProcedure || 
						x instanceof MetaView || 
						x instanceof MetaTrigger
					)
					.collect(Collectors.toList())
				).sortFromDependencies()
			) {
				ConsoleWriter.println(
					getLang().getValue("general", "restore", "droppingObject").withParams(object.getName()),
					messageLevel + 3
				);
				adapter
				.getFactoryRestore()
				.getAdapterRestore(object.getType(), adapter)
				.removeMetaObject(object);
				updateObjs.put(object);
			}

			// remove table indexes and constraints, which is step(-2) of restoreMetaObject(MetaTable)
			ConsoleWriter.println(getLang().getValue("general", "restore", "droppingTablesConstraints"), messageLevel + 2);
			for (IMetaObject table : new ScalarOf<List<IMetaObject>>(
				ipt -> {
					ipt.forEach(x -> ConsoleWriter.println(
						MessageFormat.format("{0} ({1})", x.getName(), x.getUnderlyingDbObject().getDependencies()), 
						messageLevel + 3
					));
					return ipt;
				},
				new SortedListMetaObject(
					dbObjs.entrySet().stream()
					.filter(x -> updateObjs.containsKey(x.getKey()))
					.map(Map.Entry::getValue)
					.filter(x -> x instanceof MetaTable)
					.collect(Collectors.toList())
				).sortFromDependencies()
			).value()) {
				ConsoleWriter.println(
					getLang().getValue("general", "restore", "droppingTableConstraints").withParams(table.getName()), 
					messageLevel + 3
				);
				adapter.getFactoryRestore().getAdapterRestore(DBGitMetaType.DBGitTable, adapter).restoreMetaObject(table, - 2);
			}

			if(toMakeBackup && toMakeChanges) {
				backupObjs.putAll(deleteObjs);
				backupObjs.putAll(updateObjs);
				adapter.getBackupAdapterFactory().getBackupAdapter(adapter).backupDatabase(backupObjs);
			}

			if (deleteObjs.size() != 0){
				if (toMakeChanges) ConsoleWriter.println(getLang().getValue("general", "restore", "removing"),1);
				gmdm.deleteDataBase(deleteObjs, true);
			}


			if (toMakeChanges) {
				ConsoleWriter.println(getLang().getValue("general", "restore", "restoring"),1);
			}
			gmdm.restoreDataBase(updateObjs);


		} finally {
			if (scriptOutputStream != null) {
				scriptOutputStream.flush();
				scriptOutputStream.close();
			}
			if (cmdLine.hasOption("s")) {
				String scriptName = cmdLine.getOptionValue("s");
				File file = new File(scriptName);

				if (!file.exists()) {
					ConsoleWriter.println(getLang().getValue("general", "restore", "scriptWillSaveTo").withParams(scriptName),1);
					Files.copy(autoScriptFile.toPath(), file.toPath());
				} else {
					ConsoleWriter.println(getLang().getValue("errors", "restore", "fileAlreadyExists").withParams(scriptName),1);
				}
			}
		}
		ConsoleWriter.println(getLang().getValue("general", "done"), messageLevel);
	}

	private boolean checkNeedsRestore(IMetaObject obj){
		boolean isRestore = false;
		try {
			IMetaObject dbObj = IMetaObject.create(obj.getName());
			final boolean exists = GitMetaDataManager.getInstance().loadFromDB(dbObj);
			isRestore = !exists || !dbObj.getHash().equals(obj.getHash());
		} catch (ExceptionDBGit e) {
			throw new ExceptionDBGitRunTime(e);
//			isRestore = true;
//			e.printStackTrace();
		}
		return isRestore;
	}
	private File createScriptFile() throws ExceptionDBGit, IOException {
		DBGitPath.createScriptsDir();
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		File scriptFile = new File(DBGitPath.getScriptsPath() + "script-" + format.format(new Date()) + ".sql");
//		System.out.println(scriptFile.getAbsolutePath());
		if (!scriptFile.exists()) { scriptFile.createNewFile(); }
		return scriptFile;
	}

}

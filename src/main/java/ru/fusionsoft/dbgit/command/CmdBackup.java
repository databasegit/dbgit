package ru.fusionsoft.dbgit.command;

import java.io.File;
import java.io.FileOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBBackupAdapter;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.MaskFilter;

public class CmdBackup implements IDBGitCommand {

	private Options opts = new Options();
	
	public CmdBackup() {
		opts.addOption("d", false, "Saves tabledata while backups");
		opts.addOption("s", false, "Saves backup to the specific schema, otherwise - to the current schema");
	}
	
	@Override
	public String getCommandName() {
		return "backup";
	}

	@Override
	public String getParams() {
		return "<file_mask>";
	}

	@Override
	public String getHelperInfo() {
		return "Examples:\n"
				+ "    dbgit backup <SCHEME>/<DB_OBJECT_NAME> -d -s\n"
				+ "    dbgit backup <SCHEME>/* -s";
	}

	@Override
	public Options getOptions() {
		return opts;
	}

	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));	
		
		if (cmdLine.getArgs().length != 1) {
			throw new ExceptionDBGit("Bad command. Object to backup doesn't specified!");
		}
		
		String nameObj = cmdLine.getArgs()[0];
		MaskFilter mask = new MaskFilter(nameObj);
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();		
		IMapMetaObject dbObjs = gmdm.loadDBMetaData();	
		
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		IDBAdapter adapter = AdapterFactory.createAdapter();
		
		File scriptFile = new File(DBGitPath.getScriptsPath() + "script-" + format.format(new Date()) + ".sql");
		DBGitPath.createScriptsDir();
		
		if (!scriptFile.exists()) 
			scriptFile.createNewFile();
		
		FileOutputStream scriptOutputStream = new FileOutputStream(scriptFile);
		adapter.setDumpSqlCommand(scriptOutputStream, true);
		
		IDBBackupAdapter backupAdapter = adapter.getBackupAdapterFactory().getBackupAdapter(adapter);
		backupAdapter.setToSaveData(cmdLine.hasOption("d"));
		backupAdapter.saveToSchema(cmdLine.hasOption("s"));
		
		for (IMetaObject obj : dbObjs.values()) {
			if (mask.match(obj.getName())) {	
				backupAdapter.backupDBObject(obj);
			}
		}

	}

}

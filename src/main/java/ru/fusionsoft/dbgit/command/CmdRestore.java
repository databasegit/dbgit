package ru.fusionsoft.dbgit.command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitObjectNotFound;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.core.ItemIndex;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaObjectFactory;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
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
		
		try {
			AdapterFactory.createAdapter();
		} catch (NullPointerException e) {
			ConsoleWriter.println(getLang().getValue("errors", "restore", "cantConnect"));
			System.exit(0);
		}
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		IMapMetaObject fileObjs = gmdm.loadFileMetaData();		
		IMapMetaObject updateObjs = new TreeMapMetaObject();
		IMapMetaObject deleteObjs = new TreeMapMetaObject();

		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
		
		FileOutputStream fop = null;
		FileOutputStream scriptOutputStream = null;

		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
		
		File scriptFile = new File(DBGitPath.getScriptsPath() + "script-" + format.format(new Date()) + ".sql");
		DBGitPath.createScriptsDir();
		
		if (!scriptFile.exists()) 
			scriptFile.createNewFile();
		
		scriptOutputStream = new FileOutputStream(scriptFile);

		IDBAdapter adapter = AdapterFactory.createAdapter();
		adapter.setDumpSqlCommand(scriptOutputStream, cmdLine.hasOption("r"));
		
		if (cmdLine.hasOption("s")) {
			String scriptName = cmdLine.getOptionValue("s");
			ConsoleWriter.detailsPrintLn(getLang().getValue("general", "restore", "scriptWillSaveTo").withParams(scriptName));
			
			File file = new File(scriptName);
			if (!file.exists()) {
				file.createNewFile();
				ConsoleWriter.detailsPrintLn(getLang().getValue("general", "restore", "created").withParams(scriptName));
			}
			
			fop = new FileOutputStream(file);
		}
		try {
			//delete obj
			DBGitIndex index = DBGitIndex.getInctance();			
			
			ConsoleWriter.println(getLang().getValue("general", "restore", "seekingToRemove"));
			
			for (ItemIndex item : index.getTreeItems().values()) {
				//TODO db ignore
				if (item.getIsDelete()) {
					try {
						IMetaObject obj = MetaObjectFactory.createMetaObject(item.getName());
						gmdm.loadFromDB(obj);					
						if (item.getHash().equals(obj.getHash())) {							
							deleteObjs.put(obj);
							if (deleteObjs.size() == 1)
								ConsoleWriter.println(getLang().getValue("general", "restore", "toRemove"));
							ConsoleWriter.println("    " + obj.getName());
						}
					} catch(ExceptionDBGit e) {
						LoggerUtil.getGlobalLogger().error(getLang().getValue("errors", "restore", "cantConnect") + ": " + item.getName(), e);
					}
				}
			}
			if (deleteObjs.size() == 0)
				ConsoleWriter.println(getLang().getValue("general", "restore", "nothingToRemove"));
			
			if (cmdLine.hasOption("r")) {
				ConsoleWriter.println(getLang().getValue("general", "restore", "removing"));
			}
			gmdm.deleteDataBase(deleteObjs);
			
			ConsoleWriter.println(getLang().getValue("general", "restore", "seekingToRestore"));

			for (IMetaObject obj : fileObjs.values()) {
				Boolean isRestore = false;
				try {
					IMetaObject dbObj = MetaObjectFactory.createMetaObject(obj.getName());				
					gmdm.loadFromDB(dbObj);
					
					isRestore = !dbObj.getHash().equals(obj.getHash());
					
				} catch (ExceptionDBGit e) {
					isRestore = true;
					e.printStackTrace();
				} catch (ExceptionDBGitRunTime e) {
					isRestore = true;
					e.printStackTrace();
				}
				if (isRestore) {
					//запомнили файл если хеш разный или объекта нет					
					updateObjs.put(obj);
					
					if (updateObjs.size() == 1)
						ConsoleWriter.println(getLang().getValue("general", "restore", "toRestore"));
					ConsoleWriter.println("    " + obj.getName());
				}
			}
			
			if (updateObjs.size() == 0)
				ConsoleWriter.println(getLang().getValue("general", "restore", "nothingToRestore"));
			
			if (cmdLine.hasOption("r")) {
				ConsoleWriter.println(getLang().getValue("general", "restore", "restoring"));
			}
			gmdm.restoreDataBase(updateObjs);
			
		} finally {
			if (fop != null) {
				fop.flush();
				fop.close();
			}	
			if (scriptOutputStream != null) {
				scriptOutputStream.flush();
				scriptOutputStream.close();
			}	
		}	
		ConsoleWriter.println(getLang().getValue("general", "done"));
	}


}

package ru.fusionsoft.dbgit.command;

import java.sql.Timestamp;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGit;
import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdDump implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdDump() {
		opts.addOption("a", false, getLang().getValue("help", "dump-a").toString());
		opts.addOption("f", false, getLang().getValue("help", "dump-f").toString());
		opts.addOption("u", false, getLang().getValue("help", "dump-u").toString());
	}
	
	public String getCommandName() {
		return "dump";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		return getLang().getValue("help", "dump").toString();
	}
	
	public Options getOptions() {
		return opts;
	}
	@Override
	public void execute(CommandLine cmdLine) throws Exception {		
		Boolean isAddToGit = cmdLine.hasOption('a');
		Boolean isAllDump = cmdLine.hasOption('f');
		
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
		
		GitMetaDataManager gmdm = GitMetaDataManager.getInstance();
				
		DBGitIndex index = DBGitIndex.getInctance();
		
		if (!cmdLine.hasOption("u"))
			checkVersion();
		
		ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "checking"), messageLevel);

		IMapMetaObject fileObjs = gmdm.loadFileMetaDataForce();
		
		ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "dumping"), messageLevel);
		
		for (IMetaObject obj : fileObjs.values()) {
			Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());
			ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "processing")
				.withParams(obj.getName())
				, messageLevel
			);
			String hash = obj.getHash();

			ConsoleWriter.detailsPrintln(DBGitLang.getInstance()
			    .getValue("general", "dump", "hash")
			    .withParams(hash)
			    , messageLevel+1
			);

			ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "loading"), messageLevel+1);
			if (!gmdm.loadFromDB(obj)) {
				ConsoleWriter.println(getLang().getValue("general", "dump", "cantFindInDb")
					.withParams(obj.getName())
					, messageLevel
				);
				continue;
			}
			ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "dbHash")
					.withParams(obj.getHash()), messageLevel+1
			);
			
			if (isAllDump || !obj.getHash().equals(hash)) {
				if (!obj.getHash().equals(hash))
					ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "hashesDifferent")
						, messageLevel+1
					);
				else
					ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "fSwitchFound")
						, messageLevel+1
					);
				//сохранили файл если хеш разный
				obj.saveToFile();
				ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "addToIndex"), messageLevel+1);
				index.addItem(obj);				
				ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));
				
				if (isAddToGit) {
					ConsoleWriter.detailsPrintln(getLang().getValue("general", "addToGit"), messageLevel+1);
					obj.addToGit();						
					ConsoleWriter.detailsPrintGreen(getLang().getValue("general", "ok"));
				}
			} else {
				ConsoleWriter.detailsPrintln(getLang().getValue("general", "dump", "hashesMatch"), messageLevel+1);
			}
			Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
			Long diff = timestampAfter.getTime() - timestampBefore.getTime();
			ConsoleWriter.detailsPrint(getLang().getValue("general", "time").withParams(diff.toString()));
		}
		
		index.saveDBIndex();
		if (isAddToGit) {
			ConsoleWriter.detailsPrintln(getLang().getValue("general", "addToGit"), messageLevel+1);
			index.addToGit();
		}			
		ConsoleWriter.println(getLang().getValue("general", "done"), messageLevel);
	}
}

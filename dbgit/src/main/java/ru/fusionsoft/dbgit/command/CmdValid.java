package ru.fusionsoft.dbgit.command;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdValid implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdValid() {
		opts.addOption("log", false, "Shows operation details");
	}
	
	public String getCommandName() {
		return "valid";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		//return "Command checks if dbgit data files are valid. You can get details with -log switch like this example:\n"
		//		+ "    dbgit valid -log";
		return "Example:\n"
				+ "    dbgit valid -log";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		boolean toShowLog = cmdLine.hasOption("log");
		
		//возможно за списком файлов нужно будет сходить в гит индекс
		try {
			Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData(toShowLog);
			ConsoleWriter.printlnGreen("All files are OK");
		} catch (Exception e) {
			ConsoleWriter.printlnRed(e.getMessage());
		}
		
	}

}

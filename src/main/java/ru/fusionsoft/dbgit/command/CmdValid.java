package ru.fusionsoft.dbgit.command;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGitIndex;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class CmdValid implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdValid() {
	}
	
	public String getCommandName() {
		return "valid";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		return getLang().getValue("help", "valid").toString();
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		GitMetaDataManager gmdm = GitMetaDataManager.getInstance();
		ConsoleWriter.setDetailedLog(cmdLine.hasOption("v"));
		
		checkVersion();
		
		//возможно за списком файлов нужно будет сходить в гит индекс
		try {
			Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
			ConsoleWriter.printlnGreen(getLang().getValue("general", "valid", "allOk"));
		} catch (Exception e) {
			ConsoleWriter.printlnRed(e.getMessage());
		}
		
	}

}

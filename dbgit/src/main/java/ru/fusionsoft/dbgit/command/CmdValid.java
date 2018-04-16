package ru.fusionsoft.dbgit.command;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;

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
		return "Command correct dbgit data";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		// TODO Auto-generated method stub
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		
		//возможно за списком файлов нужно будет сходить в гит индекс
		Map<String, IMetaObject> fileObjs = gmdm.loadFileMetaData();
		
		
	}

}

package ru.fusionsoft.dbgit.command;

import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;

public class CmdScript implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdScript() {
		
	}
	
	public String getCommandName() {
		return "script";
	}
	
	public String getParams() {
		return "";
	}
	
	public String getHelperInfo() {
		return "Command script";
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {
		// TODO Auto-generated method stub
		GitMetaDataManager gmdm = GitMetaDataManager.getInctance();
		

	}

}

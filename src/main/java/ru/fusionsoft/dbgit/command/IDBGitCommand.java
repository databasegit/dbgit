package ru.fusionsoft.dbgit.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Class implementation Command dbgit
 * 
 * @author mikle
 *
 */
public interface IDBGitCommand {
	public void execute(CommandLine cmdLine) throws Exception;
	
	public String getCommandName();
	
	public String getParams();
	
	public String getHelperInfo();
	
	public Options getOptions();
}

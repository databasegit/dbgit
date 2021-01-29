package ru.fusionsoft.dbgit.command;


import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;


public class CmdLink implements IDBGitCommand {
	private Options opts = new Options();
	
	public CmdLink() {
		opts.addOption("d", false, getLang().getValue("help", "link-d").toString());
		opts.addOption("ls", false, getLang().getValue("help", "link-ls").toString());
	}
	
	public String getCommandName() {
		return "link";
	}
	
	public String getParams() {
		return "<connection_string>";
	}
	
	public String getHelperInfo() {
		//return "Command creates link to database, you need to specify connection string as parameter in JDBC driver connection URL format";
		return getLang().getValue("help", "link").toString();
	}
	
	public Options getOptions() {
		return opts;
	}
	
	@Override
	public void execute(CommandLine cmdLine) throws Exception {

		DBConnection conn = DBConnection.getInstance(false);
		if(cmdLine.hasOption("ls")) {
			ConsoleWriter.printlnGreen(DBConnection.loadFileDBLink(new Properties()), messageLevel);
			return;
		}


		String[] args = cmdLine.getArgs();
		if(args == null || args.length == 0) {
			throw new ExceptionDBGit(getLang().getValue("errors", "link", "emptyLink"));			
		}

		String url = args[0];
		Properties props = CreateProperties(Arrays.copyOfRange(args, 1, args.length));

		if(conn.testingConnection(url, props)) {
			DBConnection.createFileDBLink(url, props, cmdLine.hasOption("d"));	
			DBGitPath.createDefaultDbignore(DBGitPath.getFullPath(), url, props);
		}			
	}
	
	public Properties CreateProperties(String[] args) {
		Properties props = new Properties();
		for(String prop: args){
			String[] tmp = prop.split("=");
			props.put(tmp[0], tmp[1]);
		}
		return props;
	}
	

}

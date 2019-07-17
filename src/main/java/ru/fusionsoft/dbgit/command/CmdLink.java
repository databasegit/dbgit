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
		String[] args = cmdLine.getArgs();
		
		if(args == null || args.length == 0) {
			throw new ExceptionDBGit(getLang().getValue("errors", "link", "emptyLink"));			
		}
		
		String url = args[0];
		Properties props = CreateProperties(Arrays.copyOfRange(args, 1, args.length));
		
		DBConnection conn = DBConnection.getInctance(false);
		
		if(conn.testingConnection(url, props)) {
			DBConnection.createFileDBLink(url, props);	
			DBGitPath.createDefaultDbignore(DBGitPath.getFullPath(), props.getProperty("user").toUpperCase());
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

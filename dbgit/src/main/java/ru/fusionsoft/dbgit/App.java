package ru.fusionsoft.dbgit;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.command.CmdAdd;
import ru.fusionsoft.dbgit.command.CmdDump;
import ru.fusionsoft.dbgit.command.CmdLink;
import ru.fusionsoft.dbgit.command.CmdRestore;
import ru.fusionsoft.dbgit.command.CmdRm;
import ru.fusionsoft.dbgit.command.CmdScript;
import ru.fusionsoft.dbgit.command.CmdStatus;
import ru.fusionsoft.dbgit.command.CmdValid;
import ru.fusionsoft.dbgit.command.ExceptionCmdNotFound;
import ru.fusionsoft.dbgit.command.IDBGitCommand;
import ru.fusionsoft.dbgit.utils.LoggerUtil;


/**
 * Hello world!
 *
 */
public class App 
{
	private static final Map<String, IDBGitCommand> commands;
	static {
        Map<String, IDBGitCommand> aMap = new HashMap<String, IDBGitCommand>();
        aMap.put("link", new CmdLink());
        aMap.put("dump", new CmdDump());
        aMap.put("restore", new CmdRestore());
        aMap.put("status", new CmdStatus());
        aMap.put("add", new CmdAdd());
        aMap.put("rm", new CmdRm());
        aMap.put("valid", new CmdValid());
        aMap.put("script", new CmdScript());

        commands = Collections.unmodifiableMap(aMap);
	}

	public static void executeDbGitCommand(String[] args) throws Exception {
		if (args.length == 0) {
			throw new ExceptionCmdNotFound("Command not found!");
		}
		
		String cmd = args[0].toLowerCase();
		if (!commands.containsKey(cmd)) {
			throw new ExceptionCmdNotFound("Command "+cmd+ " not found!");
		}
		
		String[] cmdParams = Arrays.copyOfRange(args, 1, args.length); 
		commands.get(cmd).execute(cmdParams);
	}
	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World dbgit!" );
        
        try {
        	executeDbGitCommand(args);
        	
        	System.out.println( "executeDbGitCommand ok!" );
        } catch (Exception e) {
        	LoggerUtil.getGlobalLogger().error(e.getMessage(), e);
        	System.out.println("dbgit error: "+e.getMessage());
        }
        
    }
    
}


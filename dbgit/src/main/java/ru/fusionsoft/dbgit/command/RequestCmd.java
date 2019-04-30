package ru.fusionsoft.dbgit.command;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

public class RequestCmd {
	public static final String FOOTER_HELPER = "";

	private static RequestCmd cmdReq = null;
	
	protected CommandMap commands = new CommandMap();
	private String currCommand = null;
	private CommandLine cmdLine = null;
	
	private RequestCmd() {
		commands.put(new CmdLink());
		commands.put(new CmdDump());
		commands.put(new CmdRestore());
		commands.put(new CmdStatus());
		commands.put(new CmdAdd());
		commands.put(new CmdRm());
        commands.put(new CmdValid());
        commands.put(new CmdHelp());
        commands.put(new CmdSynonymSchema());
        commands.put(new CmdCommit());
        commands.put(new CmdCheckout());
        commands.put(new CmdMerge());
        commands.put(new CmdPull());
        commands.put(new CmdPush());
        commands.put(new CmdInit());
        commands.put(new CmdClone());
        commands.put(new CmdRemote());
        commands.put(new CmdReset());
        commands.put(new CmdFetch());
        commands.put(new CmdConfig());
	}
	
	public static RequestCmd getInctance()  throws ExceptionDBGit {
		if (cmdReq == null) {
			cmdReq = new RequestCmd();
		}
		return cmdReq;
	}
	
	public CommandMap getCommands() {
		return commands;
	}
	
	public CommandLine parseCommand(String[] args) throws Exception {
		currCommand = args.length == 0 ? "help" : args[0].toLowerCase();		

		if (!commands.containsKey(currCommand)) {
			throw new ExceptionCmdNotFound("Command "+currCommand+ " not found!");
		}
		
		String[] cmdargs = args.length > 0 ? Arrays.copyOfRange(args, 1, args.length) : null;
		
		
		CommandLineParser clParse = new DefaultParser();
		Options opts = commands.get(currCommand).getOptions();
		addHelpOptions(opts);
		cmdLine = clParse.parse(opts, cmdargs);
		
		return cmdLine;
	}
	
	public String getCommand() {
		return currCommand;
	}
	
	public CommandLine getCommandLine() {
		return cmdLine;
	}
	
	public IDBGitCommand getCurrentCommand() {
		return commands.get(currCommand);
	}
	
	
	protected static Options addHelpOptions(Options opts) {
		if (opts.getOption("h") == null) {
			opts.addOption("h", false, "Shows this help");
		}

		if (opts.getOption("v") == null) {
			opts.addOption("v", false, "Outputs full log of command execution");
		}

		return opts;
	}	
	
	public void printHelpAboutCommand(String command) throws Exception {
		if (!commands.containsKey(command)) {
			throw new ExceptionCmdNotFound("Command "+command+ " not found!");
		}
				
		IDBGitCommand cmdObj = commands.get(command);
		
		HelpFormatter helpFormatter = new HelpFormatter();
		
        helpFormatter.printHelp(
        		"dbgit "+command+" "+cmdObj.getParams(), 
        		cmdObj.getHelperInfo(), 
        		addHelpOptions(cmdObj.getOptions()), 
        		FOOTER_HELPER, 
        		true
        );
	}
	
	
	public static void main( String[] args ) throws Exception
    {    	
    	ConsoleWriter.println( "dbgit utils - Hello!");
        
        //configureLogback();
               
        try {
        	CommandLineParser clParse = new DefaultParser();
            Options opts = new Options();
            opts.addOption("a", false, "Option A");
            opts.addOption("b", true, "Option B");
            opts.addOption("c", false, "Option C");
            opts.addOption("f", false, "Flag F");
            
            addHelpOptions(opts);

            CommandLine cmdLine = clParse.parse(opts, args); 
            System.out.println(cmdLine.getArgs().length);
            
            System.out.println(cmdLine.hasOption('a'));
            System.out.println(cmdLine.hasOption('c'));
            
            
            HelpFormatter helpFormatter = new HelpFormatter();
            //helpFormatter.setArgName("argument");
            helpFormatter.printHelp("this help", "header", opts, "footer", true);
           
            
            /*
            printHelp(
            		opts, // опции по которым составляем help
            		80, // ширина строки вывода
            		"Options", // строка предшествующая выводу
            		"-- HELP --", // строка следующая за выводом
            		3, // число пробелов перед выводом опции 
            		5, // число пробелов перед выводом опцисания опции
            		true, // выводить ли в строке usage список команд
            		System.out // куда производить вывод
            	);
            
            */
            
            for (int i = 0; i < cmdLine.getArgs().length; i++) {
            	System.out.println(cmdLine.getArgs()[i]);
            }
            
            
        	
        	ConsoleWriter.println( "execute command success!");
        } catch (Exception e) {
        	ConsoleWriter.printlnRed("Error execute dbgit: "+e.getMessage());
        	LoggerUtil.getGlobalLogger().error(e.getMessage(), e);
        }
        
    }
	

	 
}

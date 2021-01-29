package ru.fusionsoft.dbgit.command;

import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import ru.fusionsoft.dbgit.core.DBGitLang;
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
	
	public static RequestCmd getInstance()  throws ExceptionDBGit {
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
			throw new ExceptionCmdNotFound(DBGitLang.getInstance().getValue("errors", "commandNotFound").withParams(currCommand));
		}
		
		//String[] cmdargs = args.length > 0 ? Arrays.copyOfRange(args, 1, args.length) : null;
		
		CommandLineParser clParse = new DefaultParser();
		Options opts = commands.get(currCommand).getOptions();
		addHelpOptions(opts);
		
		String[] cmdargs = Arrays.stream(args)
				.skip(1)
				.map(arg -> arg.replaceAll("--([\\w,-]+)", "-" + arg.replace("-", "")))
				.toArray(String[] :: new);		
		
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
	
	
	protected static Options addHelpOptions(Options opts) throws ExceptionDBGit {
		if (opts.getOption("h") == null) {
			opts.addOption("h", false, DBGitLang.getInstance().getValue("help", "h").toString());
		}

		if (opts.getOption("v") == null) {
			opts.addOption("v", false, DBGitLang.getInstance().getValue("help", "v").toString());
		}

		return opts;
	}	
	
	public void printHelpAboutCommand(String command) throws Exception {
		if (!commands.containsKey(command)) {
			throw new ExceptionCmdNotFound(DBGitLang.getInstance().getValue("errors", "commandNotFound").withParams(command));
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
    	ConsoleWriter.println( "dbgit utils - Hello!", 0);
        
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
            
            
        	
            ConsoleWriter.println(DBGitLang.getInstance()
                .getValue("general", "commandSuccess")
                , 0
            );
        } catch (Exception e) {
        	ConsoleWriter.printlnRed(DBGitLang.getInstance()
        	    .getValue("errors", "cmdException")
        	    .withParams(e.getMessage())
        	    , 0
        	);
        	LoggerUtil.getGlobalLogger().error(e.getMessage(), e);
        }
        
    }
	

	 
}

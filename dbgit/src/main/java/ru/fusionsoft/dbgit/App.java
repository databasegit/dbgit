package ru.fusionsoft.dbgit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ru.fusionsoft.dbgit.command.CmdAdd;
import ru.fusionsoft.dbgit.command.CmdDump;
import ru.fusionsoft.dbgit.command.CmdHelp;
import ru.fusionsoft.dbgit.command.CmdLink;
import ru.fusionsoft.dbgit.command.CmdRestore;
import ru.fusionsoft.dbgit.command.CmdRm;
import ru.fusionsoft.dbgit.command.CmdScript;
import ru.fusionsoft.dbgit.command.CmdStatus;
import ru.fusionsoft.dbgit.command.CmdValid;
import ru.fusionsoft.dbgit.command.ExceptionCmdNotFound;
import ru.fusionsoft.dbgit.command.IDBGitCommand;
import ru.fusionsoft.dbgit.core.DBGitPath;
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
        aMap.put("help", new CmdHelp());

        commands = Collections.unmodifiableMap(aMap);
	}

	public static void executeDbGitCommand(String[] args) throws Exception {
		String cmd = args.length == 0 ? "help" : args[0].toLowerCase();		

		if (!commands.containsKey(cmd)) {
			throw new ExceptionCmdNotFound("Command "+cmd+ " not found!");
		}
		
		String[] cmdParams = args.length > 0 ? Arrays.copyOfRange(args, 1, args.length) : null; 
		commands.get(cmd).execute(cmdParams);
	}
	
	private static void configureLogback() throws JoranException, IOException {
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator jc = new JoranConfigurator();
		jc.setContext(context);
		context.reset();
		
		// overriding the log directory property programmatically
		String logDirProperty = ".";// ... get alternative log directory location
				
		context.putProperty("LOG_DIR", logDirProperty);		
		//System.out.println(CProperties.getProperty("log_root_level"));
		context.putProperty("log_root_level", "debug");
		
		
		ClassLoader classLoader = App.class.getClassLoader();
		File file = new File(classLoader.getResource("logback.xml").getFile());
		FileInputStream fis = new FileInputStream(file);
		jc.doConfigure(fis);
		
		
		/*
		// this assumes that the logback.xml file is in the root of the bundle.
		URL logbackConfigFileUrl = new URL("logback.xml"); 
		jc.doConfigure(logbackConfigFileUrl.openStream());
		*/
	}

	
    public static void main( String[] args ) throws Exception
    {
        System.out.println( "dbgit utils - Hello!" );
        
        //configureLogback();
               
        try {
        	executeDbGitCommand(args);
        	
        	System.out.println( "executeDbGitCommand ok!" );
        } catch (Exception e) {
        	LoggerUtil.getGlobalLogger().error(e.getMessage(), e);
        	System.out.println("dbgit error: "+e.getMessage());
        }
        
    }
    
}


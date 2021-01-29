package ru.fusionsoft.dbgit;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.cli.CommandLine;
import org.slf4j.LoggerFactory;


import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import ru.fusionsoft.dbgit.command.RequestCmd;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;


/**
 * Hello world!
 *
 */
public class App 
{
	public static void executeDbGitCommand(String[] args) throws Exception {
		RequestCmd cmd = RequestCmd.getInstance();
		
		CommandLine cmdLine = cmd.parseCommand(args);
		
		if (cmdLine.hasOption('h')) {
			cmd.printHelpAboutCommand(cmd.getCommand());
			return ;
		}
				
		cmd.getCurrentCommand().execute(cmdLine);
	}
	
	private static void configureLogback() throws JoranException, IOException {
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
		JoranConfigurator jc = new JoranConfigurator();
		jc.setContext(context);
		context.reset();
		
		// overriding the log directory property programmatically
		String logDirProperty = ".";// ... get alternative log directory location
				
		context.putProperty("LOG_DIR", logDirProperty);		
		//System.out.rintln(CProperties.getProperty("log_root_level"));
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
	
	private static void updateLogback() {
	    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();

	    ContextInitializer ci = new ContextInitializer(loggerContext);
	    URL url = ci.findURLOfDefaultConfigurationFile(true);

	    try {
	        JoranConfigurator configurator = new JoranConfigurator();
	        configurator.setContext(loggerContext);
	        loggerContext.reset();
	        configurator.doConfigure(url);
	    } catch (JoranException je) {
	        // StatusPrinter will handle this
	    }
	    StatusPrinter.printInCaseOfErrorsOrWarnings(loggerContext);		
	}
	
    public static void main( String[] args ) throws Exception
    {    	
        //configureLogback();
               
        try {        	
        	DBGitPath.createLogDir();
        	DBGitPath.deleteOldLogs();
    		
        	executeDbGitCommand(args);
        	
        } catch (Exception e) {
        	ConsoleWriter.println(DBGitLang.getInstance()
        	    .getValue("errors", "executionError")
        	    .withParams(e.getMessage())
        	    , 0
        	);
        	LoggerUtil.getGlobalLogger().error(e.getMessage(), e);
        } finally {
        	DBGitPath.clearTempDir();
		}
        
    }
    
}


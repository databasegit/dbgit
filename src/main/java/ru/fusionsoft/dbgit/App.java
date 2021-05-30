package ru.fusionsoft.dbgit;



import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import org.apache.commons.cli.CommandLine;
import org.junit.platform.commons.util.ExceptionUtils;



import ru.fusionsoft.dbgit.command.RequestCmd;
import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

public class App {
	
    public static void main( String[] args ) {
        //configureLogback();

        try {
        	DBGitPath.createLogDir();
        	DBGitPath.deleteOldLogs();
        	executeDbGitCommand(args);
        } catch (Error | Exception e) {
			final String msg = DBGitLang.getInstance()
			.getValue("errors", "executionError")
			.toString();

			System.err.println(MessageFormat.format(
				"\n{0}: {1}", 
				msg, 
				ExceptionUtils.readStackTrace(e)
			));

			LoggerUtil.getGlobalLogger().error(msg, e);
			rollbackConnection();
			System.exit(1);
		} finally {
        	DBGitPath.clearTempDir();
		}
        
    }

	public static void executeDbGitCommand(String[] args) throws Exception {
		RequestCmd cmd = RequestCmd.getInstance();
		CommandLine cmdLine = cmd.parseCommand(args);
		if (cmdLine.hasOption('h')) {
			cmd.printHelpAboutCommand(cmd.getCommand());
			return;
		}
		cmd.getCurrentCommand().execute(cmdLine);
	}
    
	private static void rollbackConnection() {
		if (DBConnection.hasInstance()) try {
			DBConnection dbConnection = DBConnection.getInstance();
			Connection connection = dbConnection.getConnect();
			if (connection != null && ! connection.isClosed()) {
				connection.rollback();
				connection.close();
			}
		} catch (Exception ex) {
			if (ex instanceof ExceptionDBGit || ex instanceof SQLException) {
				ConsoleWriter.println(DBGitLang.getInstance()
						.getValue("errors", "onExceptionTransactionRollbackError")
						.withParams(ex.getLocalizedMessage())
					, 0
				);
			} else {
				ConsoleWriter.printlnRed(
					ex.getLocalizedMessage(),
					0
				);
			}
		}
	}

//	private static void configureLogback() throws JoranException, IOException {
//		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
//		JoranConfigurator jc = new JoranConfigurator();
//		jc.setContext(context);
//		context.reset();
//		
//		// overriding the log directory property programmatically
//		String logDirProperty = ".";// ... get alternative log directory location
//				
//		context.putProperty("LOG_DIR", logDirProperty);		
//		//System.out.rintln(CProperties.getProperty("log_root_level"));
//		context.putProperty("log_root_level", "debug");
//		
//		
//		ClassLoader classLoader = App.class.getClassLoader();
//		File file = new File(classLoader.getResource("logback.xml").getFile());
//		FileInputStream fis = new FileInputStream(file);
//		jc.doConfigure(fis);
//		
//		
//		/*
//		// this assumes that the logback.xml file is in the root of the bundle.
//		URL logbackConfigFileUrl = new URL("logback.xml"); 
//		jc.doConfigure(logbackConfigFileUrl.openStream());
//		*/
//	}
//	
//	private static void updateLogback() {
//	    LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
//
//	    ContextInitializer ci = new ContextInitializer(loggerContext);
//	    URL url = ci.findURLOfDefaultConfigurationFile(true);
//
//	    try {
//	        JoranConfigurator configurator = new JoranConfigurator();
//	        configurator.setContext(loggerContext);
//	        loggerContext.reset();
//	        configurator.doConfigure(url);
//	    } catch (JoranException je) {
//	        // StatusPrinter will handle this
//	    }
//	    StatusPrinter.printInCaseOfErrorsOrWarnings(loggerContext);		
//	}
    
}


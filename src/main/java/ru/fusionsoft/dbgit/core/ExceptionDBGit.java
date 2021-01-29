package ru.fusionsoft.dbgit.core;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Base class for all Exception in dbgit project
 * 
 * @author mikle
 *
 */
public class ExceptionDBGit extends Exception {

	private static final long serialVersionUID = -4613368557825624023L;
	protected Logger logger = LoggerUtil.getLogger(this.getClass());
	protected static int messageLevel = 0;
	private Throwable cause = null;
	private String contextMessage = null;

	public ExceptionDBGit(Object msg) {
		this.setContextMessage(msg.toString());
		handleException();
	}
	public ExceptionDBGit(Object message, Throwable cause) {
		this.setContextMessage(message.toString());
		this.setCause(cause);
		handleException();
	}
	public ExceptionDBGit(Throwable cause) {
		this.setCause(cause);
		handleException();
	}

	private void handleException(){
		printMessageAndStackTrace();
		rollbackConnection();
		System.exit(1);
	}

	public void printMessageAndStackTrace(){
		if(contextMessage != null && (cause == null || !cause.getMessage().equals(contextMessage))) {
			ConsoleWriter.printlnRed(contextMessage, messageLevel);
			ConsoleWriter.printLineBreak();
		}

		if(cause != null){
			ConsoleWriter.printlnRed(cause.getLocalizedMessage(), messageLevel);
			ConsoleWriter.printLineBreak();
			ConsoleWriter.detailsPrintlnRed(ExceptionUtils.getStackTrace(cause), messageLevel);
			logger.error(contextMessage != null ? contextMessage :  cause.getMessage(), cause);

		} else {
			ConsoleWriter.detailsPrintlnRed(ExceptionUtils.getStackTrace(this), messageLevel);
			logger.error(contextMessage != null ? contextMessage :  "no error message provided..." , this);
		}
	}
	private void rollbackConnection() {
		if(DBConnection.hasInstance()) try{
			DBConnection dbConnection = DBConnection.getInstance();
			Connection connection = dbConnection.getConnect();
			if(connection != null && !connection.isClosed()){
				connection.rollback();
				connection.close();
			}
		} catch (Exception ex) {
			if(ex instanceof ExceptionDBGit || ex instanceof SQLException) {
				ConsoleWriter.println(DBGitLang.getInstance()
				    .getValue("errors", "onExceptionTransactionRollbackError")
				    .withParams(ex.getLocalizedMessage())
				    , 0
				);
			} else {
				ConsoleWriter.printlnRed(ex.getLocalizedMessage(), messageLevel);
			}
		}
	}

	public void setCause(Throwable cause) { this.cause = cause; }
	public void setContextMessage(String contextMessage) { this.contextMessage = contextMessage; }

}

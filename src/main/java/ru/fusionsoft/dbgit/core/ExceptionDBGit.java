package ru.fusionsoft.dbgit.core;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

import java.sql.SQLException;

/**
 * Base class for all Exception in dbgit project
 * 
 * @author mikle
 *
 */
public class ExceptionDBGit extends Exception {

	private static final long serialVersionUID = -4613368557825624023L;
	private Logger logger = LoggerUtil.getLogger(this.getClass());

	public ExceptionDBGit(Object msg) {
		this(msg.toString());
	}
	
	public ExceptionDBGit(String msg) {
		super(msg);
		rollbackConnection();
		ConsoleWriter.printlnRed(msg);
		ConsoleWriter.detailsPrintLn(ExceptionUtils.getStackTrace(this));
		logger.error(msg);
		System.exit(1);
	}
	
	public ExceptionDBGit(String message, Throwable cause) {

		rollbackConnection();

		if(message != null && !message.equals(cause.getMessage())) {
			ConsoleWriter.printlnRed(message);
		}

		ConsoleWriter.printlnRed(cause.getLocalizedMessage());
		ConsoleWriter.detailsPrintlnRed(ExceptionUtils.getStackTrace(cause));
		ConsoleWriter.printlnRed("");

		logger.error(message != null ? message : cause.getMessage(), cause);
		System.exit(1);
	}

	private void rollbackConnection() {
		try{
			DBConnection conn = DBConnection.getInstance();
			conn.getConnect().rollback();
		} catch (Exception ex) {
			if(ex instanceof ExceptionDBGit || ex instanceof SQLException) {
				ConsoleWriter.detailsPrintlnRed("Failed to rollback connection: " + ex.getLocalizedMessage());
			} else {
				ConsoleWriter.printlnRed(ex.getLocalizedMessage());
			}
		}
	}

	public ExceptionDBGit(Throwable cause) {
		this(null, cause);
	}

}

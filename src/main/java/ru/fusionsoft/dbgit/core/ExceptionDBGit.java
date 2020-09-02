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
		this(msg, new Exception(msg));
	}
	
	public ExceptionDBGit(String message, Throwable cause) {
		super(message, cause);
		logger.error(message, cause);
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
		ConsoleWriter.printlnRed(message);
		ConsoleWriter.detailsPrintLn(ExceptionUtils.getStackTrace(cause));
		logger.error(message, cause);
		System.exit(1);
	}
	
	public ExceptionDBGit(Throwable cause) {
		super(cause);
		logger.error(cause.getLocalizedMessage(), cause);
	}

}

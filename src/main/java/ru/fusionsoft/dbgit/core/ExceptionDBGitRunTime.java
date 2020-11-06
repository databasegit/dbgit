package ru.fusionsoft.dbgit.core;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

import java.sql.SQLException;
public class ExceptionDBGitRunTime extends RuntimeException {

	private static final long serialVersionUID = 958722213419205629L;
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	
	public ExceptionDBGitRunTime(String msg) {
		super(msg);
	}
	
	public ExceptionDBGitRunTime(String message, Throwable cause) {
		try{
			DBConnection conn = DBConnection.getInstance();
			conn.getConnect().rollback();
			//super(message, cause);
		} catch (Exception ex) {
			if(ex instanceof ExceptionDBGit || ex instanceof SQLException) {
				ConsoleWriter.detailsPrintlnRed("Failed to rollback connection: " + ex.getLocalizedMessage());
			} else {
				ConsoleWriter.printlnRed(ex.getLocalizedMessage());
			}
		}
		ConsoleWriter.printlnRed(message );

		if(cause instanceof SQLException){
			ConsoleWriter.printlnRed(ExceptionUtils.getStackTrace(cause));

		} else if ( !message.equals(cause.getMessage()) ){
			ConsoleWriter.printlnRed(cause.getMessage() );
			ConsoleWriter.detailsPrintLn(ExceptionUtils.getStackTrace(cause));
		}
		logger.error(message, cause);
		System.exit(1);

	}
	
	public ExceptionDBGitRunTime(Throwable cause) {
		this(cause.getLocalizedMessage(), cause);
	}

}

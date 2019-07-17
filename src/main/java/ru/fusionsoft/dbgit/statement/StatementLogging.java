package ru.fusionsoft.dbgit.statement;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;

public class StatementLogging implements Statement {	
	private OutputStream stream;
	private Boolean isExec;
	private Statement delegate;	
	
	private static boolean isLogging = true;
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	public static boolean isLogging() {
		return isLogging;
	}

	public static void setLogging(boolean isLogging) {
		StatementLogging.isLogging = isLogging;
	}

	public Logger getLogger() {
		return logger;
	}

	public StatementLogging(Connection conn, OutputStream stream, Boolean isExec) throws SQLException {
		delegate = conn.createStatement();
		this.stream = stream;
		this.isExec = isExec;
	}
	
	public StatementLogging(Connection conn) throws SQLException {
		this(conn, null, true);
	}	
	
	protected void log(String sql) {
		if (!isLogging()) return ;
		
		getLogger().info(sql);
	}
	
	protected void writeSql(String sql) {
		writeSql(sql, null);
	}
	
	protected void writeSql(String sql, String separator) {
		if (stream == null) return ;
		if (sql.equals("")) return;

		try {
			String buf = sql.trim();
			if (!(sql.trim().endsWith(";") || sql.trim().endsWith("/")))
				buf += ";";
				
			buf += "\n\r";
			
			if (separator != null) 
				buf += separator + "\n\r";			
			
			stream.write(buf.getBytes("UTF-8"));
		} catch (Exception e) {
			getLogger().error(DBGitLang.getInstance().getValue("errors", "errorDump").toString(), e);
		}
	}
	
	protected boolean isExecuteSql() {
		return isExec;
	}

	//Delegate functions ************************************************
	
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return delegate.unwrap(iface);
	}

	@Override
	public ResultSet executeQuery(String sql) throws SQLException {
		log(sql);
		/*
		writeSql(sql); 
		if (!isExecuteSql()) return null;
		*/
		return delegate.executeQuery(sql);
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return delegate.isWrapperFor(iface);
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		log(sql);
		writeSql(sql); 
		
		if (!isExecuteSql()) return 0;

		return delegate.executeUpdate(sql);
	}

	@Override
	public void close() throws SQLException {
		delegate.close();
	}

	@Override
	public int getMaxFieldSize() throws SQLException {
		return delegate.getMaxFieldSize();
	}

	public void setMaxFieldSize(int max) throws SQLException {
		delegate.setMaxFieldSize(max);
	}

	@Override
	public int getMaxRows() throws SQLException {
		return delegate.getMaxRows();
	}

	@Override
	public void setMaxRows(int max) throws SQLException {
		delegate.setMaxRows(max);
	}

	@Override
	public void setEscapeProcessing(boolean enable) throws SQLException {
		delegate.setEscapeProcessing(enable);
	}

	@Override
	public int getQueryTimeout() throws SQLException {
		return delegate.getQueryTimeout();
	}

	@Override
	public void setQueryTimeout(int seconds) throws SQLException {
		delegate.setQueryTimeout(seconds);
	}

	@Override
	public void cancel() throws SQLException {
		delegate.cancel();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return delegate.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		delegate.clearWarnings();
	}

	@Override
	public void setCursorName(String name) throws SQLException {
		delegate.setCursorName(name);
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return true;

		return delegate.execute(sql);
	}
	
	public boolean execute(String sql, String separator) throws SQLException {
		log(sql);
		writeSql(sql, separator);
		if (!isExecuteSql()) return true;

		return delegate.execute(sql);
	}

	@Override
	public ResultSet getResultSet() throws SQLException {
		return delegate.getResultSet();
	}

	@Override
	public int getUpdateCount() throws SQLException {
		return delegate.getUpdateCount();
	}

	@Override
	public boolean getMoreResults() throws SQLException {
		return delegate.getMoreResults();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		delegate.setFetchDirection(direction);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return delegate.getFetchDirection();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		delegate.setFetchSize(rows);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return delegate.getFetchSize();
	}

	@Override
	public int getResultSetConcurrency() throws SQLException {
		return delegate.getResultSetConcurrency();
	}

	@Override
	public int getResultSetType() throws SQLException {
		return delegate.getResultSetType();
	}

	@Override
	public void addBatch(String sql) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return ;
		
		delegate.addBatch(sql);
	}

	@Override
	public void clearBatch() throws SQLException {
		delegate.clearBatch();
	}

	@Override
	public int[] executeBatch() throws SQLException {	
		if (!isExecuteSql()) return null;
		
		return delegate.executeBatch();
	}

	@Override
	public Connection getConnection() throws SQLException {
		return delegate.getConnection();
	}
	@Override
	public boolean getMoreResults(int current) throws SQLException {
		return delegate.getMoreResults(current);
	}

	@Override
	public ResultSet getGeneratedKeys() throws SQLException {
		return delegate.getGeneratedKeys();
	}

	@Override
	public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return 0;
		
		return delegate.executeUpdate(sql, autoGeneratedKeys);
	}

	@Override
	public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return 0;
		
		return delegate.executeUpdate(sql, columnIndexes);
	}

	@Override
	public int executeUpdate(String sql, String[] columnNames) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return 0;
		
		return delegate.executeUpdate(sql, columnNames);
	}

	@Override
	public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return true;
		
		return delegate.execute(sql, autoGeneratedKeys);
	}

	@Override
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return true;
		
		return delegate.execute(sql, columnIndexes);
	}

	@Override
	public boolean execute(String sql, String[] columnNames) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return true;
		
		return delegate.execute(sql, columnNames);
	}

	@Override
	public int getResultSetHoldability() throws SQLException {
		return delegate.getResultSetHoldability();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return delegate.isClosed();
	}

	@Override
	public void setPoolable(boolean poolable) throws SQLException {
		delegate.setPoolable(poolable);
	}

	@Override
	public boolean isPoolable() throws SQLException {
		return delegate.isPoolable();
	}

	@Override
	public void closeOnCompletion() throws SQLException {
		delegate.closeOnCompletion();
	}

	@Override
	public boolean isCloseOnCompletion() throws SQLException {
		return delegate.isCloseOnCompletion();
	}

	@Override
	public long getLargeUpdateCount() throws SQLException {
		return delegate.getLargeUpdateCount();
	}

	@Override
	public void setLargeMaxRows(long max) throws SQLException {
		delegate.setLargeMaxRows(max);
	}

	@Override
	public long getLargeMaxRows() throws SQLException {
		return delegate.getLargeMaxRows();
	}

	@Override
	public long[] executeLargeBatch() throws SQLException {
		if (!isExecuteSql()) return null;
		return delegate.executeLargeBatch();
	}

	@Override
	public long executeLargeUpdate(String sql) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return 0L;
		
		return delegate.executeLargeUpdate(sql);
	}

	@Override
	public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return 0L;
		
		return delegate.executeLargeUpdate(sql, autoGeneratedKeys);
	}

	@Override
	public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return 0L;
		
		return delegate.executeLargeUpdate(sql, columnIndexes);
	}

	@Override
	public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
		log(sql);
		writeSql(sql);
		if (!isExecuteSql()) return 0L;
		
		return delegate.executeLargeUpdate(sql, columnNames);
	}	
	
}

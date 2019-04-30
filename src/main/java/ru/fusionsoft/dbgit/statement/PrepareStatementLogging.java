package ru.fusionsoft.dbgit.statement;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.axiomalaska.jdbc.DelegatingPreparedStatement;
import com.axiomalaska.jdbc.NamedParameterPreparedStatement;

public class PrepareStatementLogging extends DelegatingPreparedStatement {
	private OutputStream stream;
	private Boolean isExec;
	

	public PrepareStatementLogging(Connection conn, String sql, OutputStream stream, Boolean isExec) throws SQLException {
		super(conn.prepareStatement(sql));
		this.stream = stream;
		this.isExec = isExec;
	}
	
	public PrepareStatementLogging(Connection conn, String sql) throws SQLException {
		this(conn, sql, null, true);
	}	
	
	
	//TODO override all execute function


}

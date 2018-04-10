package ru.fusionsoft.dbgit.statement;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.axiomalaska.jdbc.DelegatingPreparedStatement;
import com.axiomalaska.jdbc.NamedParameterPreparedStatement;

public class StatementLogging extends DelegatingPreparedStatement {
	public StatementLogging(Connection conn, String sql) throws SQLException {
		super(conn.prepareStatement(sql));
	}
}

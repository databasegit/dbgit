package ru.fusionsoft.dbgit.integration.primitives.connection;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.SafeScalarOf;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;

public class ConnectionEnvelope implements Connection {
    private final SafeScalar<Connection> origin;

    public ConnectionEnvelope(Scalar<Connection> origin) {
        this.origin = new SafeScalarOf<>(new StickyScalar<>(origin));
    }

    @Override
    public Statement createStatement() throws SQLException {
        return origin.value().createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return origin.value().prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return origin.value().prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return origin.value().nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        origin.value().setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return origin.value().getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        origin.value().commit();
    }

    @Override
    public void rollback() throws SQLException {
        origin.value().rollback();
    }

    @Override
    public void close() throws SQLException {
        origin.value().close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return origin.value().isClosed();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return origin.value().getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        origin.value().setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return origin.value().isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        origin.value().setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return origin.value().getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        origin.value().setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return origin.value().getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return origin.value().getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        origin.value().clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return origin.value().createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return origin.value().prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return origin.value().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return origin.value().getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        origin.value().setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        origin.value().setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return origin.value().getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return origin.value().setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return origin.value().setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        origin.value().rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        origin.value().releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return origin.value().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return origin.value().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return origin.value().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return origin.value().prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return origin.value().prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return origin.value().prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return origin.value().createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return origin.value().createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return origin.value().createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return origin.value().createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return origin.value().isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        origin.value().setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        origin.value().setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return origin.value().getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return origin.value().getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return origin.value().createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return origin.value().createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        origin.value().setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return origin.value().getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        origin.value().abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        origin.value().setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return origin.value().getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return origin.value().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return origin.value().isWrapperFor(iface);
    }
}

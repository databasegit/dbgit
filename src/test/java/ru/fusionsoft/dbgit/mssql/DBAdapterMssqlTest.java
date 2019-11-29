package ru.fusionsoft.dbgit.mssql;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;


public class DBAdapterMssqlTest {

    public static Properties testProps;
    private DBAdapterMssql testAdapter;
    private Connection testConnection;

    static{
        testProps = new Properties();
        testProps.setProperty("url", "jdbc:sqlserver://localhost:1433;databaseName=master;integratedSecurity=false;");
        testProps.setProperty("user", "test");
        testProps.setProperty("password", "test");
        testProps.put("characterEncoding", "UTF-8");
    }

    @Before
    public void setUp() throws Exception {
        String url = testProps.getProperty("url");
        testProps.remove("url");
        testConnection = DriverManager.getConnection(url, testProps);
        testConnection.setAutoCommit(false);
        testAdapter = (DBAdapterMssql)  AdapterFactory.createAdapter(testConnection);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getSchemes() {
        testAdapter.getSchemes();
    }
}
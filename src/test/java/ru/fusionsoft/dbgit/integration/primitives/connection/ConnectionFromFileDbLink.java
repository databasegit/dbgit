package ru.fusionsoft.dbgit.integration.primitives.connection;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.Properties;

public class ConnectionFromFileDbLink extends ConnectionEnvelope {
    public ConnectionFromFileDbLink(Path pathToFileDbLink) {
        super(() -> {
            final Properties properties = new Properties();
            properties.load(new FileInputStream(pathToFileDbLink.toFile()));
            return DriverManager.getConnection(
                properties.getProperty("url"),
                properties.getProperty("user"),
                properties.getProperty("password")
            );
        });
    }
}

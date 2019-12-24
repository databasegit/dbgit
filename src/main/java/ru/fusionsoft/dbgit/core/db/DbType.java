package ru.fusionsoft.dbgit.core.db;

public enum DbType {
    ORACLE("oracle"),
    POSTGRES("postgresql"),
    MYSQL("mysql");

    private String dbName;

    DbType(String name) {
        dbName = name;
    }

    @Override
    public String toString() {
        return dbName;
    }
}

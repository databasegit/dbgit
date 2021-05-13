package ru.fusionsoft.dbgit.core.db;

import java.util.Arrays;

public enum FieldType {
    BINARY("binary"),
    BOOLEAN("boolean"),
    DATE("date"),
    NATIVE("native"),
    NUMBER("number"),
    STRING("string"),
    STRING_NATIVE("string native"),
    ENUM("enum"),
    TEXT("text"),
    UNDEFINED("");

    private String typeName;

    FieldType(String name) {
        typeName = name;
    }

    @Override
    public String toString() {
        return typeName;
    }


    public static FieldType fromString(String name) {
        return Arrays.stream(FieldType.values())
                .filter(ft -> ft.typeName.equals(name.toLowerCase()))
                .findFirst()
                .orElse(NATIVE);
    }
}

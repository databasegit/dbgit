package ru.fusionsoft.dbgit.integration.primitives.files;

import java.io.IOException;
import java.util.Map;

public interface TextResourceGroup {
    void add(String name, String content) throws IOException;
    void clean() throws IOException;
    TextResource file(String... name);
    Map<String, TextResource> all() throws IOException;
}

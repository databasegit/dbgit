package ru.fusionsoft.dbgit.integration.primitives.files;


import java.io.IOException;

public interface TextResource {
    String text() throws IOException;
    TextResource updateText(String content) throws IOException;
    TextResource delete() throws IOException;

}

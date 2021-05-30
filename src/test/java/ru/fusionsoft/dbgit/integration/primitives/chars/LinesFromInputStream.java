package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;

public class LinesFromInputStream implements Lines {
    private final Scalar<List<String>> listScalar;
    public LinesFromInputStream(InputStream origin, String codepageName) {
        this.listScalar = new StickyScalar<>(
            () -> {
                try (
                    final BufferedReader reader = new BufferedReader(new InputStreamReader(origin, codepageName));
                ) {
                    final LinkedList<String> lines = new LinkedList<>();
                    String line;
                    while (( line = reader.readLine() ) != null) {
                        lines.add(line);
                    }
                    return lines;
                }
            }
        );
    }
    
    public LinesFromInputStream(InputStream origin) {
        this(origin, "default");
    }

    @Override
    public final List<String> list() throws Exception {
        return listScalar.value();
    }
}

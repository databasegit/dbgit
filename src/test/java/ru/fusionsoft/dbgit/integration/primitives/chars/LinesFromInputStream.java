package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;
import ru.fusionsoft.dbgit.integration.primitives.chars.specific.test.Lines;

public class LinesFromInputStream implements Lines {
    private final Scalar<List<String>> listScalar;
    public LinesFromInputStream(InputStream origin, String codepageName) {
        this.listScalar = new StickyScalar<>(
            () -> {
                try (
                    final BufferedReader reader = new BufferedReader(
                        codepageName.equals("default")
                            ? new InputStreamReader(origin)
                            : new InputStreamReader(origin, codepageName)
                    )
                ) { 
                    return reader.lines().collect(Collectors.toList());
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

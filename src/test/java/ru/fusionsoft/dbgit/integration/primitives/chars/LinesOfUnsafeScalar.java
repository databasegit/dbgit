package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.util.Arrays;
import java.util.List;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;

public class LinesOfUnsafeScalar implements Lines {
    private final Scalar<CharSequence> charsScalar;
    
    public LinesOfUnsafeScalar(CharSequence charSequence) {
        this.charsScalar = charSequence::toString;
    }

    @Override
    public final List<String> list() throws Exception {
        return Arrays.asList(String.valueOf(charsScalar.value()).split("\n"));
    }
}

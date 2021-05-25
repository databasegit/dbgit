package ru.fusionsoft.dbgit.integration.primitives.chars.specific.test;

import java.util.Arrays;
import java.util.List;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;

public class LinesOf implements Lines {
    private final Scalar<String> stringScalar;
    
    public LinesOf(CharSequence charSequence) {
        this.stringScalar = charSequence::toString;
    }

    @Override
    public final List<String> list() throws Exception {
        return Arrays.asList(stringScalar.value().split("\n"));
    }
}

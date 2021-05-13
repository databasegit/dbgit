package ru.fusionsoft.dbgit.integration.primitives.chars;

import ru.fusionsoft.dbgit.integration.primitives.Function;
import ru.fusionsoft.dbgit.integration.primitives.Scalar;
import ru.fusionsoft.dbgit.integration.primitives.StickyScalar;

public class CharsOf<X> extends CharSequenceEnvelope {
    public CharsOf(Function<X, CharSequence> extraction, X source) {
        super(new StickyScalar<CharSequence>(extraction, source));
    }
    
    public CharsOf(Scalar<CharSequence> scalar) {
        super(new StickyScalar<>(scalar));
    }
}

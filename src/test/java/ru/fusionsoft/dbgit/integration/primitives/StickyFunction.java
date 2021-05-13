package ru.fusionsoft.dbgit.integration.primitives;

import java.util.HashMap;
import java.util.Map;

public class StickyFunction<X, Y> implements Function<X, Y> {
    private final Function<X, Y> origin;
    private final Map<X, Y> cachedValues;

    private StickyFunction(Function<X, Y> origin, Map<X, Y> cachedValues) {
        this.origin = origin;
        this.cachedValues = cachedValues;
    }

    public StickyFunction(Function<X,Y> origin){
        this(origin, new HashMap<>());
    }

    public StickyFunction(Scalar<Y> origin){
        this((x) -> origin.value());
    }

    @Override
    public Y value(X arg) throws Exception {
        if (!cachedValues.containsKey(arg)) {
            cachedValues.put(arg, origin.value(arg));
        }
        return cachedValues.get(arg);
    }
}

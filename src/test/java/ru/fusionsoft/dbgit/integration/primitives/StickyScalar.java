package ru.fusionsoft.dbgit.integration.primitives;

public class StickyScalar<Y> implements Scalar<Y> {
    private final Function<Boolean, Y> origin;

    public StickyScalar(Scalar<Y> origin) {
        this.origin = new StickyFunction<>(origin);
    }

    public <X> StickyScalar(Function<X, Y> origin, X value) {
        this(
            new Scalar<Y>() {
                @Override
                public Y value() throws Exception {
                    return origin.value(value);
                }
            }
        );
    }

    @Override
    public final Y value() throws Exception {
        return this.origin.value(true);
    }
}

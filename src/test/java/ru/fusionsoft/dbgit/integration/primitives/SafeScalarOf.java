package ru.fusionsoft.dbgit.integration.primitives;

public class SafeScalarOf<Y> implements SafeScalar<Y> {
    private final Scalar<Y> origin;

    public SafeScalarOf(Scalar<Y> origin) {
        this.origin = origin;
    }

    public <X> SafeScalarOf(Function<X, Y> origin, X value){
        this( () -> origin.value(value) ) ;
    }

    @Override
    public final Y value() {
        try {
            return origin.value();
        } catch (Exception e) {
            throw new SafeScalarError(e);
        }
    }

    public static class SafeScalarError extends Error {
        public SafeScalarError(Throwable cause) {
            super("Error occurs while constructing safe scalar value", cause);
        }
    }
}

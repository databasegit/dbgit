package ru.fusionsoft.dbgit.integration.primitives;

public class PatchedScalar<T> implements Scalar<T> {
    private final T origin;
    private final Patch<T> patch;

    public PatchedScalar(T origin, Patch<T> patch) {
        this.origin = origin;
        this.patch = patch;
    }

    @Override
    public final T value() throws Exception {
        this.patch.apply(origin);
        return origin;
    }
}

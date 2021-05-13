package ru.fusionsoft.dbgit.integration.primitives;

import java.util.Arrays;
import java.util.Collection;

public class PatchSequental<T> implements Patch<T> {
    private final Collection<Patch<T>> patches;

    public PatchSequental(final Collection<Patch<T>> patches) {
        this.patches = patches;
    }
    
    @SafeVarargs
    public PatchSequental(final Patch<T>... patches) {
        this(Arrays.asList(patches));
    }

    @Override
    public final void apply(final T root) throws Exception {
        for (final Patch<T> patch : patches) {
            patch.apply(root);
        }
    }
}

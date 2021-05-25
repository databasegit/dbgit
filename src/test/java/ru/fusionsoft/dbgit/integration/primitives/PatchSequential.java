package ru.fusionsoft.dbgit.integration.primitives;

import java.util.Arrays;
import java.util.Collection;

public class PatchSequential<T> implements Patch<T> {
    private final Collection<Patch<T>> patches;

    public PatchSequential(final Collection<Patch<T>> patches) {
        this.patches = patches;
    }
    
    @SafeVarargs
    public PatchSequential(final Patch<T>... patches) {
        this(Arrays.asList(patches));
    }

    @Override
    public final void apply(final T root) throws Exception {
        for (final Patch<T> patch : patches) {
            patch.apply(root);
        }
    }
}

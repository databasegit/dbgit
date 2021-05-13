package ru.fusionsoft.dbgit.integration.primitives;

public class SimpleTest<Subj> implements Test<Subj> {
    private final Function<Subj, Boolean> predicate;
    private final String description;

    public SimpleTest(String description, Function<Subj, Boolean> predicate) {
        this.description = description;
        this.predicate = predicate;
    }
    
    public SimpleTest(Function<Subj, Boolean> predicate) {
        this.description = "test just runs without errors";
        this.predicate = predicate;
    }

    @Override
    public final boolean value(Subj subj) throws Exception {
        return this.predicate.value(subj);
    }

    @Override
    public final String description() {
        return this.description;
    }
}

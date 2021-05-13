package ru.fusionsoft.dbgit.integration.primitives;

public interface Test<Subj> {
    String description();
    boolean value(Subj subj) throws Exception;
}

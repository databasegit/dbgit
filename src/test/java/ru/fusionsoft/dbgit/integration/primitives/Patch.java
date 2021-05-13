package ru.fusionsoft.dbgit.integration.primitives;

/*
    A consumer, but can throw checked
 */
public interface Patch<T> {
    void apply(T root) throws Exception;
}

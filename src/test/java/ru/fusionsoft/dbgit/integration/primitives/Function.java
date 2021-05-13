package ru.fusionsoft.dbgit.integration.primitives;


public interface Function<X, Y>{
    Y value(X source) throws Exception;
}

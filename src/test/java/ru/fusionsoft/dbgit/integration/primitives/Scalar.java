package ru.fusionsoft.dbgit.integration.primitives;


public interface Scalar<T>  {
    T value() throws Exception;

    Scalar DONT_CARE = () -> { throw new Exception("Not implemented"); };
}

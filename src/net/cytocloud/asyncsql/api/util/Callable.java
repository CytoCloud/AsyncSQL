package net.cytocloud.asyncsql.api.util;

@FunctionalInterface
public interface Callable<T> {

    T call();

}

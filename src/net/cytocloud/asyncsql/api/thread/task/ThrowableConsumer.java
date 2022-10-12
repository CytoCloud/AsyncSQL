package net.cytocloud.asyncsql.api.thread.task;

@FunctionalInterface
public interface ThrowableConsumer<E, T extends Throwable> {

    void accept(E e) throws T;

}

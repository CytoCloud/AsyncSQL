package net.cytocloud.asyncsql.api.thread.task;

import lombok.Getter;
import net.cytocloud.asyncsql.api.thread.task.future.exception.FutureExceptionManager;
import org.jetbrains.annotations.NotNull;

public abstract class Task<E, I extends Throwable> {

    @Getter @NotNull FutureExceptionManager<I> futureExceptionManager = new FutureExceptionManager<>();
    public abstract void execute(@NotNull E e) throws I;

}

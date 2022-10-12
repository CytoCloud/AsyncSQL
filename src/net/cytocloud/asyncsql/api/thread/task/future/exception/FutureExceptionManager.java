package net.cytocloud.asyncsql.api.thread.task.future.exception;

import lombok.Getter;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class FutureExceptionManager<T extends Throwable> {

    @Getter @Setter
    private @NotNull Consumer<FutureException> onThrowConsumer = t -> {};
    @Getter
    private @Nullable T throwable;
    private final @NotNull List<Consumer<@NotNull T>> consumers = new ArrayList<>();

    public FutureExceptionManager() {
        this.throwable = null;
    }

    public void response(@NotNull T throwable) {
        this.throwable=throwable;
        this.consumers.forEach(consumer -> consumer.accept(throwable));
    }

    public void async(@NotNull Consumer<@NotNull T> consumer) {
        if(this.throwable != null) {
            consumer.accept(this.throwable);
            return;
        }

        consumers.add(consumer);
    }

    public boolean checkForThrowing() {
        if(this.throwable == null) return true;

        throwException();
        return false;
    }

    public void throwException() throws FutureException {
        if(this.throwable == null) throw new NullPointerException("There is no throwable");
        FutureException e = new FutureException(this.throwable);
        getOnThrowConsumer().accept(e);

        throw e;
    }

}

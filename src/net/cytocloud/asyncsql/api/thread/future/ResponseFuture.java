package net.cytocloud.asyncsql.api.thread.future;

import lombok.Getter;
import net.cytocloud.asyncsql.api.thread.task.ThrowableConsumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class ResponseFuture<T> {

    private @Nullable transient T value;
    @Getter
    private final @Nullable Consumer<SQLException> exceptionConsumer;
    private final List<ThrowableConsumer<T, SQLException>> consumers = new ArrayList<>();

    public ResponseFuture() {
        this.exceptionConsumer=null;
    }

    public ResponseFuture(@Nullable Consumer<SQLException> exceptionConsumer) {
        this.exceptionConsumer=exceptionConsumer;
    }

    public void async(@NotNull ThrowableConsumer<T, SQLException> consumer) {
        consumers.add(consumer);
    }

    public @Nullable T syncUntil(long milliseconds) {
        long check = System.currentTimeMillis() + milliseconds;

        while(getValue() == null && System.currentTimeMillis() < check);

        return value;
    }

    public @NotNull T syncUntilElse(long milliseconds, T t) {
        T response = syncUntil(milliseconds);
        return response == null ? t : response;
    }

    public @NotNull T sync() {
        while(getValue() == null) {}
        return value;
    }

    public void response(@NotNull T t) {
        this.value=t;
        consumers.forEach(consumer -> {
            try {
                consumer.accept(t);
            } catch (SQLException e) {
                if(exceptionConsumer != null)
                    getExceptionConsumer().accept(e);
            }
        });
    }

    public synchronized T getValue() {
        return value;
    }
}

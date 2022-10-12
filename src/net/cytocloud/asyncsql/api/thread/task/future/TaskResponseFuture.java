package net.cytocloud.asyncsql.api.thread.task.future;

import lombok.Getter;
import lombok.Setter;
import net.cytocloud.asyncsql.api.thread.task.Task;
import net.cytocloud.asyncsql.api.thread.task.ThrowableConsumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class TaskResponseFuture<Response> {

    @Getter @Setter
    private @NotNull Consumer<SQLException> asyncExceptionHandler = Throwable::printStackTrace;
    @Getter @Setter
    private @NotNull Task<Connection, SQLException> task;
    private final List<ThrowableConsumer<@NotNull Response, SQLException>> responseConsumers = new ArrayList<ThrowableConsumer<@NotNull Response, SQLException>>();
    private @Nullable Response response;

    public TaskResponseFuture() {}
    public TaskResponseFuture(@NotNull Task<Connection, SQLException> task) {
        this.task=task;
    }

    /**
     * Response to all consumers or every open sync waiters
     * @param response The response to send
     */
    public void response(@NotNull Response response) {
        this.response=response;
        this.responseConsumers.forEach(responseConsumer -> {
            try {
                responseConsumer.accept(response);
            } catch (SQLException e) {
                this.asyncExceptionHandler.accept(e);
            }
        });
    }

    /**
     * Synchronously get of response (Blocking the current thread until a response was received)
     * @apiNote Only interrupts when the executed task causes an exception
     * @return The response
     */
    public @NotNull Response sync() {
        while(this.response == null && task.getFutureExceptionManager().checkForThrowing()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return this.response;
    }

    /**
     * Synchronously get of response (Blocking the current thread until a response was received or the entered time is over)
     * @apiNote Only interrupts when the executed task causes an exception or the time was reached
     * @param milliseconds After that time the method will return null
     * @return The response or null when the time was reached but nothing was received
     */
    public @Nullable Response syncUntil(long milliseconds) {
        long time = System.currentTimeMillis() + milliseconds;

        while(this.response == null && task.getFutureExceptionManager().checkForThrowing() && time > System.currentTimeMillis()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return this.response;
    }

    /**
     * Synchronously get of response (Blocking the current thread until a response was received or the entered time is over)
     * @apiNote Only interrupts when the executed task causes an exception or the time was reached
     * @param milliseconds After that time the method will return the entered response
     * @param response The response will be returned after the time was reached
     * @return The response or when the time is over return the entered response
     */
    public @NotNull Response syncUntilElse(long milliseconds, @NotNull Response response) {
        final @Nullable Response syncUntil = syncUntil(milliseconds);

        if(syncUntil == null) return response;
        return syncUntil;
    }

    /**
     * Asynchronously get of the response (When a response receives the entered consumer will be called)
     * @param responseConsumer The consumer which can receive responses
     * @return An instance of this
     */
    public @NotNull TaskResponseFuture<Response> async(@NotNull ThrowableConsumer<@NotNull Response, SQLException> responseConsumer) {
        if(this.response != null) {
            try {
                responseConsumer.accept(response);
            } catch (SQLException e) {
                this.asyncExceptionHandler.accept(e);
            }
            return this;
        }

        this.responseConsumers.add(responseConsumer);
        return this;
    }

}

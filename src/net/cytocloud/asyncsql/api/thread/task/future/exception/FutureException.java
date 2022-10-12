package net.cytocloud.asyncsql.api.thread.task.future.exception;

import org.jetbrains.annotations.NotNull;

public class FutureException extends RuntimeException {

    public FutureException(@NotNull Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return "An async exception was thrown";
    }

}

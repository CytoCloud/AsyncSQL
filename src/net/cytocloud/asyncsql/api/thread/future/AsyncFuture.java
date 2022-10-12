package net.cytocloud.asyncsql.api.thread.future;

import net.cytocloud.asyncsql.api.thread.AsyncSQLThreadWorker;
import net.cytocloud.asyncsql.api.util.Callable;
import org.jetbrains.annotations.NotNull;

import java.util.UUID;

public class AsyncFuture {

    public static <T> @NotNull ResponseFuture<T> await(@NotNull Callable<Boolean> consumer, @NotNull T obj) {
        ResponseFuture<T> t = new ResponseFuture<>();

        String uuid = UUID.randomUUID().toString();

        AsyncSQLThreadWorker.runAsyncRepeating(uuid, () -> {
            if(!consumer.call()) return;
            AsyncSQLThreadWorker.removeWorkProcess(uuid);

            t.response(obj);
        }, 10);

        return t;
    }

}

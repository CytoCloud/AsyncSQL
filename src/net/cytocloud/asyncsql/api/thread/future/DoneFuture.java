package net.cytocloud.asyncsql.api.thread.future;

import net.cytocloud.asyncsql.api.thread.task.future.exception.FutureException;
import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DoneFuture {

    private final List<Callable> callables = new ArrayList<>();
    private boolean isDone = false;
    private SQLException exception = null;

    public void done() {
        callables.forEach(Callable::call);
        isDone = true;
    }

    public @NotNull DoneFuture async(@NotNull Callable done) {
        if(isDone) {
            done.call();
            return this;
        }

        callables.add(done);
        return this;
    }

    public void sync() {
        while(!isDone && exception == null){
            try{
                Thread.sleep(10);
            }catch(InterruptedException ignored){}
        }

        if(isDone) return;
        throw new FutureException(exception);
    }

    public void syncUntil(long milliseconds) {
        long check = System.currentTimeMillis() + milliseconds;

        while(!isDone && System.currentTimeMillis() < check && exception == null) {
            try{
                Thread.sleep(10);
            }catch(InterruptedException ignored){}
        }

        if(isDone) return;
        throw new FutureException(exception);
    }

    public void notifyException(@NotNull SQLException exception) {
        this.exception=exception;
    }

    @FunctionalInterface
    public interface Callable {
        void call();
    }

}

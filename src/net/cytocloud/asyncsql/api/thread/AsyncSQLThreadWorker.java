package net.cytocloud.asyncsql.api.thread;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AsyncSQLThreadWorker {

    private static final @Nullable Thread thread;
    private static final Map<String, Runnable> runners = new ConcurrentHashMap<>();
    private static final List<String> toRemove = new ArrayList<>();
    private static boolean isActive = false;

    static {
        if(Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.getName().equals("AsyncSQL-ThreadWorker"))) {
            thread = null;
        }else{
            isActive = true;

            thread = new Thread(() -> {
                while(isActive) {
                    runners.values().forEach(Runnable::run);

                    if(toRemove.isEmpty()) continue;

                    toRemove.forEach(runners::remove);
                    toRemove.clear();
                }
            });

            thread.setName("AsyncSQL-ThreadWorker");
            thread.start();
        }
    }

    /**
     * Run something on repeat
     * @param runnable The runnable
     * @param period The time in milliseconds
     */
    public static synchronized void runAsyncRepeating(@NotNull String name, @NotNull Runnable runnable, long period) {
        AtomicLong executeTime = new AtomicLong(System.currentTimeMillis() + period);

        addWorkProcess(name, () -> {
            if(executeTime.get() == System.currentTimeMillis()) {
                runnable.run();
                executeTime.set(System.currentTimeMillis() + period);
            }
        });
    }

    /**
     * @param runnable The run method
     * @throws RuntimeException When the thread is null or the runnable is already registered
     */
    public static synchronized void runAsync(@NotNull Runnable runnable) {
        String name = UUID.randomUUID().toString();

        addWorkProcess(name, runnable);
        removeWorkProcess(name);
    }

    /**
     * @param runnable The run method
     * @param name The name of the runnable
     * @throws RuntimeException When the thread is null or the runnable is already registered
     */
    public static synchronized void addWorkProcess(@NotNull String name, @NotNull Runnable runnable) {
        if(thread == null) throw new RuntimeException("The thread is null");
        if(runners.containsKey(name)) throw new RuntimeException("The runnable is already registered");

        runners.put(name, runnable);
    }

    /**
     * @param name The name of the runnable
     */
    public static synchronized void removeWorkProcess(@NotNull String name) {
        if(runners.containsKey(name)) toRemove.add(name);
    }

    public static boolean hasWorkProcess(@NotNull String name) {
        return runners.containsKey(name);
    }

    public static boolean isThreadActive() {
        return isActive;
    }

    public static void interrupt() {
        isActive = false;
        if(thread != null)
            thread.interrupt();
    }


}

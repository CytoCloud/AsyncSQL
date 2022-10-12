package net.cytocloud.asyncsql.api.connection;

import lombok.AccessLevel;
import lombok.Getter;
import net.cytocloud.asyncsql.AsyncSQL;
import net.cytocloud.asyncsql.api.connection.cache.CacheManager;
import net.cytocloud.asyncsql.api.connection.table.Table;
import net.cytocloud.asyncsql.api.thread.AsyncSQLThreadWorker;
import net.cytocloud.asyncsql.api.thread.future.DoneFuture;
import net.cytocloud.asyncsql.api.thread.future.ResponseFuture;
import net.cytocloud.asyncsql.api.thread.task.Task;
import net.cytocloud.asyncsql.api.thread.task.ThrowableConsumer;
import net.cytocloud.asyncsql.api.thread.task.future.TaskResponseFuture;
import net.cytocloud.asyncsql.api.util.ConnectionInformation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.*;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class AsyncConnection {

    @Getter(AccessLevel.NONE)
    private final Queue<Task<Connection, SQLException>> tasks = new ConcurrentLinkedQueue<>();
    private final @NotNull UUID workerUUID = UUID.randomUUID();
    private final CacheManager cacheManager = new CacheManager();
    private final ConnectionInformation connectionInformation;
    private final boolean reconnectActivated;

    /**
     * @param hostname The hostname of the server
     * @param port The port
     * @param database The database name
     * @param username The username to login
     * @param password The password to login
     */
    public AsyncConnection(@NotNull String hostname, int port, @NotNull String database, @NotNull String username, @NotNull String password) {
        this(new ConnectionInformation(hostname, port, database, username, password), false);
    }

    public AsyncConnection(@NotNull ConnectionInformation information, @NotNull boolean reconnect) {
        this.reconnectActivated=reconnect;
        this.connectionInformation=information;
        initConnection(0, new DoneFuture());
        AsyncSQL.registerConnection(this);
    }

    /**
     * Execute a task on the current sql connection
     * @param task A task to get the connection
     * @return An instance of the entered parameter
     */
    public synchronized @NotNull Task<Connection, SQLException> executeConnectionTask(@NotNull ThrowableConsumer<Connection, SQLException> task){
        Task<Connection, SQLException> rt = new Task<>() {
            @Override
            public void execute(@NotNull Connection connection) throws SQLException {
                task.accept(connection);
            }
        };

        rt.getFutureExceptionManager().setOnThrowConsumer(e -> {
            if(!reconnectActivated) return;
            initConnection(0, new DoneFuture().async(() -> executeConnectionTask(task)));
        });

        tasks.add(rt);
        return rt;
    }

    public @NotNull DoneFuture update(@NotNull String query) {
        DoneFuture future = new DoneFuture();

        executeConnectionTask(connection -> {
            Statement statement = connection.createStatement();
            statement.executeUpdate(query);
            statement.close();

            future.done();
        }).getFutureExceptionManager().async(future::notifyException);

        return future;
    }

    public @NotNull TaskResponseFuture<ResultSet> query(@NotNull String query) {
        TaskResponseFuture<ResultSet> f = new TaskResponseFuture<>();

        f.setTask(executeConnectionTask(connection -> {
            Statement statement = connection.createStatement();
            f.response(statement.executeQuery(query));
            statement.closeOnCompletion();
        }));

        return f;
    }

    /**
     * Get a sql table (When not exists return null)
     * @param name The name of the table
     * @return A representation of the table
     */
    public @Nullable Table getTable(@NotNull String name) {
        if(existsTable(name).sync())
            return new Table(name, this);

        return null;
    }

    /**
     * Create a table (Returns the table when exist)
     * @param name The name of the table
     * @param columnProperties The column information's. As example: (Name text, Age text). Only the things in the ().
     * @return A representation of the table
     */
    public @NotNull Table createTable(@NotNull String name, @NotNull String columnProperties) {
        Table table = getTable(name);
        if(table != null) return table;

        update("CREATE TABLE `" + name + "` (" + columnProperties + ")");

        return new Table(name, this);
    }

    /**
     * Check if a table exists
     * @param name The name of the table
     * @return true on exists
     */
    public @NotNull ResponseFuture<Boolean> existsTable(@NotNull String name) {
        ResponseFuture<Boolean> b = new ResponseFuture<>();

        query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE `TABLE_NAME` = '" + name + "'").async(r -> b.response(r.next()));

        return b;
    }

    /**
     * Checks if the connection is still connected
     * @return true when connected
     */
    public @NotNull ResponseFuture<Boolean> isConnected() {
        ResponseFuture<Boolean> b = new ResponseFuture<>();

        executeConnectionTask(connection -> b.response(connection.isClosed()));

        return b;
    }

    /** Disconnect the connection */
    public void disconnect() {
        executeConnectionTask(Connection::close);
        AsyncSQLThreadWorker.removeWorkProcess(workerUUID.toString());
    }

    private DoneFuture initConnection(int attempt, @NotNull DoneFuture doneFuture) {
        String hostname = getConnectionInformation().hostname();
        String database = getConnectionInformation().database();
        String username = getConnectionInformation().username();
        String password = getConnectionInformation().password();
        int port = getConnectionInformation().port();

        final ResponseFuture<Boolean> future = new ResponseFuture<>();

        AsyncSQLThreadWorker.runAsync(() -> {
            Connection con;

            try {
                con = DriverManager.getConnection("jdbc:mysql://" + hostname +":"+port+"/" + database + "?user=" + username + "&password=" + password + "&useSSL=true&autoReconnect=true");
                doneFuture.done();
                future.response(false);
            } catch (SQLException e) {
                future.response(true);
                return;
            }

            if(AsyncSQLThreadWorker.hasWorkProcess(workerUUID.toString()))
                AsyncSQLThreadWorker.removeWorkProcess(workerUUID.toString());

            AsyncSQLThreadWorker.addWorkProcess(workerUUID.toString(), () -> {
                while(!tasks.isEmpty()) {
                    Task<Connection, SQLException> task = tasks.poll();
                    try {
                        task.execute(con);
                    } catch (SQLException e) {
                        task.getFutureExceptionManager().response(e);
                    }
                }
            });
        });

        boolean b = future.syncUntilElse(10000, true);
        if(!b) return doneFuture;

        if(attempt >= 3) throw new RuntimeException("Couldn't reach the server");
        initConnection(attempt+1, doneFuture);

        return doneFuture;
    }

}

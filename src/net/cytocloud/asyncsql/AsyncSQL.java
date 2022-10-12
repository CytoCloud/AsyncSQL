package net.cytocloud.asyncsql;

import net.cytocloud.asyncsql.api.connection.AsyncConnection;
import net.cytocloud.asyncsql.api.connection.cache.CacheManager;
import net.cytocloud.asyncsql.api.connection.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class AsyncSQL {

    private static final List<AsyncConnection> connections = new ArrayList<>();

    /**
     * Create a new async connection
     * @param hostname The hostname
     * @param port The port
     * @param database The database
     * @param username The username
     * @param password The password
     * @return An instance of the connection
     * @throws RuntimeException When the server couldn't be reached
     */
    public static @NotNull AsyncConnection create(String hostname, int port, String database, String username, String password) {
        return new AsyncConnection(hostname, port, database, username, password);
    }

    /**
     * @return a list of all connections which are connected
     */
    public static @NotNull List<AsyncConnection> getConnections() {
        return connections.stream().filter(connection -> connection.isConnected().syncUntilElse(500, false)).collect(Collectors.toList());
    }

    /**
     * @return A list of all connections which were established (connected & disconnected)
     */
    public static @NotNull List<AsyncConnection> getAllConnections() {
        return new ArrayList<>(connections);
    }

    /**
     * Get all cache managers of connected connections
     */
    public static @NotNull List<CacheManager> getAllCacheManagers() {
        List<CacheManager> managers = new ArrayList<>();

        getConnections().forEach(connection -> managers.add(connection.getCacheManager()));

        return managers;
    }

    /**
     * Upload all caches of every connection
     */
    public static void uploadAllCaches() {
        getAllCacheManagers().forEach(CacheManager::uploadAll);
    }

    /**
     * <b>Only for Internal use</b>
     */
    public static void registerConnection(@NotNull AsyncConnection connection) {
        if(!connections.contains(connection))
        connections.add(connection);
    }

}

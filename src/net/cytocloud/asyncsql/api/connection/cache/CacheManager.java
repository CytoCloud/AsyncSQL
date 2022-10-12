package net.cytocloud.asyncsql.api.connection.cache;

import net.cytocloud.asyncsql.api.connection.cache.cached.CachedTable;
import net.cytocloud.asyncsql.api.connection.cache.cached.CachedTableRow;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class CacheManager {

    private final List<CachedTable> tables = new ArrayList<>();

    /**
     * <b>Only for Internal use</b>
     */
    public void registerCachedTable(@NotNull CachedTable cachedTable) {
        this.tables.add(cachedTable);
    }

    /**
     * Get all cached tables which were created
     * @return A copy of the list
     */
    public @NotNull List<CachedTable> getCachedTables() {
        return new ArrayList<>(tables);
    }

    /**
     * Get all information about the cache
     */
    public @NotNull Map<CachedTable, Map<String, CachedTableRow>> getFullCachedTableData() {
        Map<CachedTable, Map<String, CachedTableRow>> data = new HashMap<>();

        getCachedTables().forEach(cachedTable -> data.put(cachedTable, cachedTable.getCache()));

        return data;
    }

    /**
     * Upload all cached table data
     */
    public void uploadAll() {
        getFullCachedTableData().forEach((cachedTable, map) -> cachedTable.upload());
    }

    /**
     * Download all cached table data
     */
    public void downloadAll() {
        getFullCachedTableData().forEach((cachedTable, map) -> cachedTable.download());
    }

    /**
     * Upload all with an entry filter
     * @param filter Filter the uploading entries
     */
    public void uploadFiltered(Predicate<? super Map.Entry<CachedTable, Map<String, CachedTableRow>>> filter) {
        getFullCachedTableData().entrySet().stream().filter(filter).forEach(entry -> entry.getKey().upload());
    }

    /**
     * Download all with an entry filter
     * @param filter Filter the downloading entries
     */
    public void downloadFiltered(Predicate<? super Map.Entry<CachedTable, Map<String, CachedTableRow>>> filter) {
        getFullCachedTableData().entrySet().stream().filter(filter).forEach(entry -> entry.getKey().download());
    }

}

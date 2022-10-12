package net.cytocloud.asyncsql.api.connection.cache.cached;

import lombok.Getter;
import net.cytocloud.asyncsql.api.connection.cache.cached.resolver.ColumnDataResolver;
import net.cytocloud.asyncsql.api.connection.table.Table;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@Getter
public class CachedTable {

    private final @NotNull Table table;
    private final @NotNull String selectionColumn;
    private final @NotNull Map<String, CachedTableRow> cache = new HashMap<>();
    private final @NotNull ColumnDataResolver resolver;
    private final @NotNull ExpirationAction action;
    private final long expiration;

    protected CachedTable(@NotNull ExpirationAction action, long expiration, @NotNull Table table, @NotNull String selectionColumn, @NotNull ColumnDataResolver resolver) {
        this.action=action;
        this.table=table;
        this.selectionColumn=selectionColumn;
        this.resolver=resolver;
        this.expiration=expiration;
        table.getConnection().getCacheManager().registerCachedTable(this);
    }

    /**
     * Load and get a table value (when nothing found: insert)
     * @param value The value
     * @param columnValues The columns associated with the values
     * @return A cached table row
     */
    public @NotNull CachedTableRow getOrDefault(@NotNull String value, @NotNull Map<String, String> columnValues) {
        CachedTableRow r = get(value);

        if(r != null)
            return r;

        table.insert(columnValues).sync();
        return Objects.requireNonNull(get(value));
    }

    /**
     * Load and get a table value
     * @param value The value
     * @return A cached table row or null when there is nothing in the table
     * @throws IllegalAccessError When the value can't be downloaded
     */
    public @Nullable CachedTableRow get(@NotNull String value) {
        if(has(value)) {
            final CachedTableRow row = cache.get(value);

            if(row.isExpired()) {
                if(action == ExpirationAction.DELETE) {
                    cache.remove(value);
                    return null;
                }else {
                    row.download();
                    return row;
                }
            }

            return row;
        }

        CachedTableRow row = getFromSQL(value);

        if(row == null) return null;

        cache.put(value, row);

        return row;
    }

    /**
     * Remove a value from the cache and from the sql
     * @param value The value
     * @return The removed cached table row
     */
    public @Nullable CachedTableRow remove(@NotNull String value) {
        CachedTableRow row = cache.remove(value);

        if(hasOnSQL(value))
            this.table.remove("`" + getSelectionColumn() + "` = " + getResolver().export(getSelectionColumn(), value));

        return row;
    }

    /**
     * Remove a value from the cache
     * @param value The value
     */
    public @Nullable CachedTableRow removeFromCache(@NotNull String value) {
        return cache.remove(value);
    }

    /**
     * Check if the cache contains the value
     * @param value The value
     * @return true when exists
     */
    public boolean has(@NotNull String value) {
        return cache.containsKey(value);
    }

    public boolean hasOnSQL(@NotNull String value) {
        return getTable().hasEntry("`"+ getSelectionColumn() + "` = " + getResolver().export(getSelectionColumn(), value)).sync();
    }

    /**
     * Add a value to the cache (Or overwrite it when exist)
     * @param value The value
     * @param columnValues The column values (Without serialized sql format)
     * @return The associated cached table row
     */
    public @NotNull CachedTableRow set(@NotNull String value, @NotNull Map<String, Object> columnValues) {
        CachedTableRow row = get(value);

        if(row == null) {
            row = new CachedTableRow(this, columnValues, value, selectionColumn);

            getCache().put(value, row);
            row.upload();

            return row;
        }

        row.refreshExpiration();
        row.getValues().clear();
        row.getValues().putAll(columnValues);

        return row;
    }

    /**
     * Upload all data
     */
    public void upload() {
        cache.forEach((s, r) -> r.upload());
    }

    /**
     * Download all data
     */
    public void download() {
        cache.forEach((s, r) -> r.download());
    }

    public @Nullable CachedTableRow getFromSQL(@NotNull String value) {
        return CachedTableRow.fromResultSet(this, value, selectionColumn, table.selectAll("`" + selectionColumn + "` = '" + value + "'", 1), resolver);
    }

    public static CachedTable from(@NotNull ExpirationAction action, @NotNull Table table, @NotNull String selectionColumn, @NotNull ColumnDataResolver resolver) {
        return new CachedTable(action, -1, table, selectionColumn, resolver);
    }

    public static CachedTable from(@NotNull ExpirationAction action, long expirationMS, @NotNull Table table, @NotNull String selectionColumn, @NotNull ColumnDataResolver resolver) {
        return new CachedTable(action, expirationMS, table, selectionColumn, resolver);
    }


}

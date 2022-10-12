package net.cytocloud.asyncsql.api.connection.cache.cached.keyvalue;

import lombok.Getter;
import net.cytocloud.asyncsql.api.connection.cache.cached.CachedTable;
import net.cytocloud.asyncsql.api.connection.cache.cached.CachedTableRow;
import net.cytocloud.asyncsql.api.connection.cache.cached.ExpirationAction;
import net.cytocloud.asyncsql.api.connection.cache.cached.resolver.ColumnDataResolver;
import net.cytocloud.asyncsql.api.connection.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;

@Getter
public class KVStorage extends CachedTable {

    private final @NotNull String secondColumn;

    public KVStorage(@NotNull ExpirationAction action, long expiration, @NotNull Table table, @NotNull String selectionColumn, @NotNull ColumnDataResolver resolver, @NotNull String secondColumn) {
        super(action, expiration, table, selectionColumn, resolver);
        this.secondColumn = secondColumn;
    }

    public @NotNull CachedTableRow set(@NotNull String value, @NotNull Object key) {
        Map<String, Object> values = new HashMap<>();

        values.put(getSelectionColumn(), value);
        values.put(getSecondColumn(), key);

        return this.set(value, values);
    }

    public @NotNull Object getOrDefault(@NotNull String key, @NotNull String value) {
        HashMap<String, String> map = new HashMap<>();

        map.put(getSelectionColumn(), getResolver().export(getSelectionColumn(), key));
        map.put(getSecondColumn(), getResolver().export(getSecondColumn(), value));

        return getOrDefault(key, map).get(getSecondColumn());
    }

}

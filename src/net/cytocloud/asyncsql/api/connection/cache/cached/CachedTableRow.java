package net.cytocloud.asyncsql.api.connection.cache.cached;

import lombok.Getter;
import net.cytocloud.asyncsql.api.connection.cache.cached.resolver.ColumnDataResolver;
import net.cytocloud.asyncsql.api.thread.task.future.TaskResponseFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

@Getter
public class CachedTableRow {

    private final @NotNull CachedTable table;
    private final @NotNull Map<String, Object> values;
    private final @NotNull String value, selectionColumn;
    private long currentTime;

    protected CachedTableRow(@NotNull CachedTable table, @NotNull Map<String, Object> values, @NotNull String value, @NotNull String selectionColumn) {
        this.values=values;
        this.value=value;
        this.table=table;
        this.selectionColumn=selectionColumn;
        this.currentTime=System.currentTimeMillis();
    }

    public boolean isExpired() {
        long e = this.getTable().getExpiration();

        if(e == -1) return false;
        return System.currentTimeMillis() >= currentTime + e;
    }

    public <T> T get(String column) {
        return (T) values.get(column);
    }

    public Object getObject(String column) {
        return values.get(column);
    }

    /**
     * Uploading the current cached values
     */
    public void upload() {
        if(isOnSQL()) {
            getTable().getTable().updateMultiple(exportSQLFormat(), "`" + selectionColumn + "` = '" + value + "'");
            return;
        }

        getTable().getTable().insert(exportSQLFormat());
    }

    /**
     * Downloads from the sql
     * @throws IllegalAccessError When the value wasn't found
     */
    public void download() {
        CachedTableRow row = this.table.getFromSQL(this.value);
        if(row == null) throw new IllegalAccessError("Unable to download the table row from the sql (Nothing found for value \"" + value + "\")");

        this.refreshExpiration();
        values.clear();
        this.values.putAll(row.getValues());
    }

    public @NotNull Map<String, String> exportSQLFormat() {
        final Map<String, String> map = new HashMap<>();
        final ColumnDataResolver r = this.getTable().getResolver();

        this.values.forEach((s, o) -> map.put(s, r.export(s, o)));

        return map;
    }

    /**
     * Refreshes the expiration time
     */
    public void refreshExpiration() {
        this.currentTime = System.currentTimeMillis();
    }

    /**
     * Check if this value is on the sql
     * @return true when exists
     */
    public boolean isOnSQL() {
        return this.table.getFromSQL(this.value) != null;
    }

    public static @Nullable CachedTableRow fromResultSet(@NotNull CachedTable table, @NotNull String selectionColumn, @NotNull String value, @NotNull TaskResponseFuture<ResultSet> result, @NotNull ColumnDataResolver resolver) {
        return fromResultSet(table, value, selectionColumn, result.sync(), resolver);
    }

    public static @Nullable CachedTableRow fromResultSet(@NotNull CachedTable table, @NotNull String selectionColumn, @NotNull String value, @NotNull ResultSet result, @NotNull ColumnDataResolver resolver) {
        final Map<String, Object> values = new HashMap<>();

        try{
            if(!result.next()) return null;

            for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
                String name = result.getMetaData().getColumnLabel(i);
                values.put(name, resolver.resolve(name, result));
            }

            return new CachedTableRow(table, values, value, selectionColumn);
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}

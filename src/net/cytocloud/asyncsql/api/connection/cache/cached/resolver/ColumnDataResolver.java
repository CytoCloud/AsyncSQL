package net.cytocloud.asyncsql.api.connection.cache.cached.resolver;

import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ColumnDataResolver {

    @NotNull Object resolve(@NotNull String column, @NotNull ResultSet set) throws SQLException;
    @NotNull String export(@NotNull String column, @NotNull Object object);

}

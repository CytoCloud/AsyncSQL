package net.cytocloud.asyncsql.api.connection.cache.cached.cooldown;

import net.cytocloud.asyncsql.api.connection.AsyncConnection;
import net.cytocloud.asyncsql.api.connection.cache.cached.ExpirationAction;
import net.cytocloud.asyncsql.api.connection.cache.cached.keyvalue.KVStorage;
import net.cytocloud.asyncsql.api.connection.cache.cached.resolver.ColumnDataResolver;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public class CooldownStorage extends KVStorage {

    protected CooldownStorage(long expiration, @NotNull AsyncConnection connection, @NotNull String table) {
        super(ExpirationAction.DOWNLOAD, expiration, connection.createTable(table, "UUID varchar(36), Cooldown long"), "UUID", new ColumnDataResolver() {

            @Override
            public @NotNull Object resolve(@NotNull String column, @NotNull ResultSet set) throws SQLException {
                switch(column) {
                    case "UUID":
                        return set.getString(column);
                    case "Cooldown":
                        return set.getLong(column);
                    default: throw new NullPointerException(column);
                }
            }

            @Override
            public @NotNull String export(@NotNull String column, @NotNull Object object) {
                switch(column) {
                    case "UUID":
                        return "'" + object + "'";
                    case "Cooldown":
                        return "" + object;
                    default: throw new NullPointerException(column);
                }
            }

        }, "Cooldown");
    }

    /**
     * Check if a cooldown is already expired
     * @param uuid The uuid
     * @return true when expired
     */
    public boolean isCooldownExpired(@NotNull String uuid) {
        return System.currentTimeMillis() >= getCooldown(uuid);
    }

    /**
     * Set a new cooldown
     * @param uuid The uuid
     * @param milliseconds When should the cooldown expire
     */
    public void setCooldown(@NotNull String uuid, long milliseconds) {
        this.set(uuid, System.currentTimeMillis()+milliseconds).upload();
    }

    /**
     * Removes a cooldown
     * @param uuid The uuid
     */
    public void removeCooldown(@NotNull String uuid) {
        this.remove(uuid);
    }

    /**
     * Get a cooldown from an uuid
     * @param uuid The uuid
     * @return -1 when no cooldown is set or the cooldown which was set
     */
    public long getCooldown(@NotNull String uuid) {
        return (long) getOrDefault(uuid, ""+-1);
    }

    public long getTimeLeft(@NotNull String uuid) {
        return getCooldown(uuid) - System.currentTimeMillis();
    }

    public @NotNull String formatTimeLeft(@NotNull String toFormat, long timeLeft) {
        long milliseconds = timeLeft;
        long seconds = 0, minutes = 0, hours = 0, days = 0, weeks = 0, months = 0, years = 0;

        while(milliseconds >= 1000) {
            milliseconds-=1000;
            seconds++;
        }

        while(seconds >= 60){
            seconds-=60;
            minutes++;
        }

        while(minutes >= 60){
            minutes-=60;
            hours++;
        }

        while(hours >= 24){
            hours-=24;
            days++;
        }

        while(days >= 7){
            days-=7;
            weeks++;
        }

        while(weeks >= 4){
            weeks-=4;
            months++;
        }

        while(months >= 12){
            weeks-=12;
            years++;
        }

        return toFormat.
        replace("{milliseconds}", ""+milliseconds).
        replace("{seconds}", ""+seconds).
        replace("{minutes}", ""+minutes).
        replace("{hours}", ""+hours).
        replace("{days}", ""+days).
        replace("{weeks}", ""+weeks).
        replace("{months}", ""+months).
        replace("{years}", ""+years);
    }

    /**
     * Create a cooldown storage
     * @param expiration The expiration time
     * @param connection The connection to get the table
     * @param tableName The name of the table
     * @return The cooldown storage
     */
    public static @NotNull CooldownStorage create(long expiration, @NotNull AsyncConnection connection, @NotNull String tableName) {
        return new CooldownStorage(expiration, connection, tableName);
    }


}

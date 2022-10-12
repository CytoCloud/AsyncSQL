package net.cytocloud.asyncsql.api.connection.table.serialize.parser;

import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface ColumnParser {

    /**
     * @param type The type of the annotated field
     * @param columnName The column name ({@link net.cytocloud.asyncsql.api.connection.table.serialize.TableColumn})
     * @param set The result set of the SELECT-Statement
     * @return The deserialized object
     * @throws SQLException When a sql error occurs
     * @throws InvalidTypeException When the entered type can't be handled
     */
    @NotNull Object parse(@NotNull Class<?> type, String columnName, ResultSet set) throws SQLException, InvalidTypeException;

    /**
     * @param fieldValue The value associated with the field
     * @param type The type of the annotated field
     * @return A string representation
     * @throws InvalidTypeException When the entered type can't be handled
     */
    @NotNull String export(@NotNull Object fieldValue, @NotNull Class<?> type) throws InvalidTypeException;

    static ColumnParser defaultParser() {
        return new ColumnParser() {
            @Override
            public @NotNull Object parse(@NotNull Class<?> type, String columnName, ResultSet set) throws SQLException, InvalidTypeException {
                if (String.class.equals(type)) {
                    return set.getString(columnName);
                }

                if (int.class.equals(type)) {
                    return set.getInt(columnName);
                }

                if (boolean.class.equals(type)) {
                    return set.getBoolean(columnName);
                }

                if (double.class.equals(type)) {
                    return set.getDouble(columnName);
                }

                if (long.class.equals(type)) {
                    return set.getLong(columnName);
                }

                if (short.class.equals(type)) {
                    return set.getShort(columnName);
                }

                throw new InvalidTypeException(type);
            }

            @Override
            public @NotNull String export(@NotNull Object fieldValue, @NotNull Class<?> type) throws InvalidTypeException {
                if (String.class.equals(type)) {
                    return "'" + fieldValue + "'";
                }

                if (int.class.equals(type)) {
                    return "" + (int) fieldValue;
                }

                if (boolean.class.equals(type)) {
                    return "" + (boolean) fieldValue;
                }

                if (double.class.equals(type)) {
                    return "" + (double) fieldValue;
                }

                if (long.class.equals(type)) {
                    return "" + (long) fieldValue;
                }

                if (short.class.equals(type)) {
                    return "" + (short) fieldValue;
                }

                throw new InvalidTypeException(type);
            }

        };
    }


    class InvalidTypeException extends Exception {

        @Getter
        private final Class<?> type;

        public InvalidTypeException(Class<?> type) {
            this.type=type;
        }

        @Override
        public String getMessage() {
            return "The type \"" + type.getName() + "\" couldn't be handled";
        }

    }

}

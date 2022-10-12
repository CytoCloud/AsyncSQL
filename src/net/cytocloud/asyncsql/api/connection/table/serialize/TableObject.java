package net.cytocloud.asyncsql.api.connection.table.serialize;

import lombok.Getter;
import net.cytocloud.asyncsql.api.connection.AsyncConnection;
import net.cytocloud.asyncsql.api.connection.table.Table;
import net.cytocloud.asyncsql.api.connection.table.serialize.parser.ColumnParser;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

@Getter
public abstract class TableObject {

    private final @NotNull Table table;
    private final @NotNull ColumnParser columnParser;
    private final @NotNull String condition;
    private boolean finishedLoading = false;

    /**
     * @param table A table
     * @param condition The condition to get/submit the data
     * @param columnParser A parser to parse {@link TableColumn} fields
     */
    public TableObject(@NotNull Table table, @NotNull ColumnParser columnParser, @NotNull String condition) {
        this.table=table;
        this.condition=condition;
        this.columnParser=columnParser;
    }

    /**
     * @param connection The connection
     * @param name The name of the table
     * @param condition The condition to get/submit the data
     * @param columnParser A parser to parse {@link TableColumn} fields
     */
    public TableObject(@NotNull AsyncConnection connection, @NotNull String name, @NotNull ColumnParser columnParser, @NotNull String condition) {
        this(Objects.requireNonNull(connection.getTable(name)), columnParser, condition);
    }

    /**
     * @param set The result set to fill in the Fields
     */
    public void fillResults(@NotNull ResultSet set) {
        try {
            if(!set.next()) return;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        getAnnotatedFields().forEach(field -> {
            try {
                field.setAccessible(true);

                if(Modifier.isFinal(field.getModifiers())) return;

                Class<?> type = field.getType();

                Object o = columnParser.parse(type, field.getDeclaredAnnotation(TableColumn.class).columnName(), set);

                if(!type.isPrimitive()) {
                    field.set(o, this);
                    return;
                }

                if (int.class.equals(type)) {
                    field.setInt(this, (Integer) o);
                }

                if (boolean.class.equals(type)) {
                    field.setBoolean(this, (Boolean) o);
                }

                if (double.class.equals(type)) {
                    field.setDouble(this, (Double) o);
                }

                if (long.class.equals(type)) {
                    field.setLong(this, (Long) o);
                }

                if (short.class.equals(type)) {
                    field.setShort(this, (Short) o);
                }

                field.setAccessible(false);
            } catch (SQLException | ColumnParser.InvalidTypeException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        });

        finishedLoading = true;
    }

    /**
     * @return A list of all annotated fields {@link TableColumn}
     */
    public @NotNull List<Field> getAnnotatedFields() {
        return Arrays.stream(getClass().getDeclaredFields()).filter(field -> Arrays.stream(field.getDeclaredAnnotations()).anyMatch(annotation -> annotation instanceof TableColumn)).collect(Collectors.toList());
    }

    /**
     * @return A map with the column name + the field
     */
    public @NotNull Map<String, Field> getColumnNameToField() {
        HashMap<String, Field> map = new HashMap<>();

        getAnnotatedFields().forEach(field -> map.put(field.getDeclaredAnnotation(TableColumn.class).columnName(), field));

        return map;
    }

    /**
     * @return A map with column name + the encoded object (as string)
     */
    public @NotNull Map<String, String> getColumnValues() {
        HashMap<String, String> values = new HashMap<>();
        Map<String, Field> cntf = getColumnNameToField();

        for (String column : table.getColumns().sync().values()) {
            try {
                Field f = cntf.get(column);

                f.setAccessible(true);
                values.put(column, columnParser.export(f.get(this), f.getType()));
            } catch (IllegalAccessException | ColumnParser.InvalidTypeException e) {
                throw new RuntimeException(e);
            }
        }

        return values;
    }

    /**
     * Submit any changes that are made to the fields
     * UPDATE TABLE `table_name` SET COLUMN1 = VALUE1, COLUMN2 = VALUE2, ... WHERE condition
     */
    public void submit() {
        if(!table.hasEntry(condition).sync()) {
            table.insert(getColumnValues());
            return;
        }

        table.updateMultiple(getColumnValues(), condition);
    }

    /**
     * Load from database
     * @see #fillResults(ResultSet)
     */
    public void load(){
        finishedLoading = false;
        this.fillResults(table.selectAll(condition).sync());
    }


}

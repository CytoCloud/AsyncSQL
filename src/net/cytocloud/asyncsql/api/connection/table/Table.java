package net.cytocloud.asyncsql.api.connection.table;

import lombok.Getter;
import net.cytocloud.asyncsql.api.connection.AsyncConnection;
import net.cytocloud.asyncsql.api.thread.future.DoneFuture;
import net.cytocloud.asyncsql.api.thread.future.ResponseFuture;
import net.cytocloud.asyncsql.api.thread.task.future.TaskResponseFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class Table {

    private final String name;
    private final AsyncConnection connection;

    public Table(@NotNull String name, @NotNull AsyncConnection connection) {
        this.name=name;
        this.connection=connection;
    }

    /**
     * @param condition To check for
     * @return If the table has the entered entry
     */
    public ResponseFuture<Boolean> hasEntry(@NotNull String condition) {
        ResponseFuture<Boolean> f = new ResponseFuture<>();

        selectAll(condition).async(resultSet -> f.response(resultSet.next()));

        return f;
    }

    /**
     * INSERT INTO `table_name` (columns) VALUES (values)
     * @param columns The columns
     * @param values The values associated to the columns
     */
    public @NotNull DoneFuture insert(@NotNull String columns, @NotNull String values) {
        return connection.update("INSERT INTO `" + name + "` (" + columns + ") VALUES (" + values + ")");
    }

    /**
     * INSERT INTO `table_name` (columns) VALUES (values)
     * @param columnValues The values associated with the columns
     * @apiNote The values aren't in the '' format
     */
    public @NotNull DoneFuture insert(@NotNull Map<String, String> columnValues) {
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        for(String k : columnValues.keySet()) {
            columns.append("`").append(k).append("`").append(", ");
            values.append(columnValues.get(k)).append(", ");
        }

        String c = columns.toString();
        String v = values.toString();

        if(c.endsWith(", ")) c = c.substring(0, c.length()-2);
        if(v.endsWith(", ")) v = v.substring(0, v.length()-2);

        return connection.update("INSERT INTO `" + name + "` (" + c + ") VALUES (" + v + ")");
    }

    /**
     * UPDATE `table_name` SET 'column' = newValue WHERE condition <br>
     * or <br>
     * UPDATE `table_name` SET 'column' = newValue
     * @apiNote The values aren't in the '' format
     * @param column The column
     * @param newValue The new value
     * @param condition The condition (behind WHERE)
     */
    public @NotNull DoneFuture updateSingle(@NotNull String column, @NotNull String newValue, @Nullable String condition) {
        if(condition == null) {
            return connection.update("UPDATE `" + name + "` SET `" + column + "` = " + newValue + "");
        }else{
            return connection.update("UPDATE `" + name + "` SET `" + column + "` = " + newValue + " WHERE " + condition);
        }
    }

    /**
     * UPDATE `table_name` SET `COLUMN1` = VALUE1 WHERE condition
     * @apiNote The values aren't in the '' format
     * @param condition The condition (behind WHERE)
     * @param columnToValueMap A map which keys are column's (and the associated new value as value)
     */
    public @NotNull DoneFuture updateMultiple(@NotNull Map<String, String> columnToValueMap, @Nullable String condition) {
        StringBuilder toSet = new StringBuilder();

        columnToValueMap.forEach((k,v) -> toSet.append("`" + k + "` = " + v + ", "));

        String ts = toSet.toString();

        if(ts.endsWith(", ")) ts = ts.substring(0, ts.length()-2);

        if(condition != null) {
            return connection.update("UPDATE `" + name + "` SET " + ts + " WHERE " + condition);
        }else{
            return connection.update("UPDATE `" + name + "` SET " + ts);
        }
    }

    /**
     * UPDATE `table_name` SET 'column' = newValue WHERE `replaceColumn` = replaceValue
     * @param column The column
     * @param newValue The new value
     * @param replaceColumn The column to replace
     * @param replaceValue The value to check
     * @apiNote The values aren't in the '' format
     */
    public @NotNull DoneFuture updateReplace(@NotNull String column, @NotNull String newValue, @Nullable String replaceColumn, @Nullable String replaceValue) {
        return connection.update("UPDATE `" + name + "` SET `" + column + "` = " + newValue + " WHERE `" + replaceColumn + "` = " + replaceValue + "");
    }

    /**
     * SELECT `column` FROM `table_name` WHERE condition
     * @param column The column to access
     * @param condition The WHERE statement
     * @return A future object of the result
     */
    public @NotNull TaskResponseFuture<ResultSet> select(@NotNull String column, @Nullable String condition) {
        if(condition != null) return connection.query("SELECT `" + column + "` FROM `"+name+"` WHERE " + condition);
        return connection.query("SELECT `" + column + "` FROM `"+name+"`");
    }

    /**
     * Select all with the entered condition
     * @param condition The condition
     * @return A future object of the results
     */
    public @NotNull TaskResponseFuture<ResultSet> selectAll(@Nullable String condition) {
        if(condition == null) {
            return connection.query("SELECT * FROM `" + name +"`");
        }else {
            return connection.query("SELECT * FROM `" + name +"` WHERE " + condition);
        }
    }

    /**
     * Select all with the entered condition and a limit
     * @param condition The condition
     * @param limit The limit
     * @return A future object of the results
     */
    public @NotNull TaskResponseFuture<ResultSet> selectAll(@Nullable String condition, int limit) {
        if(condition == null) {
            return connection.query("SELECT * FROM `" + name +"` LIMIT " + limit);
        }else {
            return connection.query("SELECT * FROM `" + name +"` WHERE " + condition + " LIMIT " + limit);
        }
    }

    /**
     * @return A map with the column name associated with the column data type
     */
    public @NotNull ResponseFuture<Map<String, String>> getColumns() {
        ResponseFuture<Map<String, String>> f = new ResponseFuture<>();

        //SELECT DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where table_schema = ’yourDatabaseName’ and table_name = ’yourTableName’

        connection.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + name + "' ORDER BY ORDINAL_POSITION").async(s -> {
            Map<String, String> columns = new HashMap<>();

            while(s.next())
                columns.put(s.getString("COLUMN_NAME"), s.getString("DATA_TYPE"));

            f.response(columns);
        });

        return f;
    }

    /**
     * ALTER TABLE `table_name` ADD column <br>
     * <b>Example:</b> ALTER TABLE `table_name` ADD Age int
     * @param column The column to add
     */
    public @NotNull DoneFuture addColumn(@NotNull String column){
        return connection.update("ALTER TABLE `"+ name + "` ADD " + column);
    }

    /**
     * TRUNCATE TABLE `table_name`
     */
    public @NotNull DoneFuture clear() {
        return connection.update("TRUNCATE TABLE `" + name + "`");
    }

    /**
     * DROP TABLE `table_name`
     */
    public @NotNull DoneFuture delete() {
        return connection.update("DROP TABLE `"+name+"`");
    }

    /**
     * DELETE FROM `table_name` WHERE CONDITION
     * @param condition The condition
     */
    public @NotNull DoneFuture remove(@NotNull String condition) {
        return connection.update("DELETE FROM `" + name + "` WHERE " + condition);
    }


}

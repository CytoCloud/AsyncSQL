package net.cytocloud.asyncsql.api.util;

public record ConnectionInformation(String hostname, int port, String database, String username, String password) {

}
package net.cytocloud.asyncsql.api.connection.cache.cached;

public enum ExpirationAction {

    DOWNLOAD,   //When a cache value is expired, download the current from the sql
    DELETE      //When a cache value is expired, delete the cache value

}

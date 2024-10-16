// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.util;

import com.amazonaws.ClientConfiguration;

public final class AWSGlueConfig {

    private AWSGlueConfig() {
    }

    public static final String AWS_GLUE_ENDPOINT = "aws.hive.metastore.glue.endpoint";
    public static final String AWS_REGION = "aws.hive.metastore.glue.region";
    public static final String AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS
            = "aws.catalog.credentials.provider.factory.class";

    public static final String AWS_GLUE_MAX_RETRY = "aws.hive.metastore.glue.max-error-retries";
    public static final int DEFAULT_MAX_RETRY = 5;

    public static final String AWS_GLUE_MAX_CONNECTIONS = "aws.hive.metastore.glue.max-connections";
    public static final int DEFAULT_MAX_CONNECTIONS = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

    public static final String AWS_GLUE_CONNECTION_TIMEOUT = "aws.hive.metastore.glue.connection-timeout";
    public static final int DEFAULT_CONNECTION_TIMEOUT = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;

    public static final String AWS_GLUE_SOCKET_TIMEOUT = "aws.hive.metastore.glue.socket-timeout";
    public static final int DEFAULT_SOCKET_TIMEOUT = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;

    public static final String AWS_GLUE_DB_CACHE_ENABLE = "aws.hive.metastore.glue.cache.db.enable";
    public static final String AWS_GLUE_DB_CACHE_SIZE = "aws.hive.metastore.glue.cache.db.size";
    public static final String AWS_GLUE_DB_CACHE_TTL_MINS = "aws.hive.metastore.glue.cache.db.ttl-mins";

    public static final String AWS_GLUE_TABLE_CACHE_ENABLE = "aws.hive.metastore.glue.cache.table.enable";
    public static final String AWS_GLUE_TABLE_CACHE_SIZE = "aws.hive.metastore.glue.cache.table.size";
    public static final String AWS_GLUE_TABLE_CACHE_TTL_MINS = "aws.hive.metastore.glue.cache.table.ttl-mins";
}

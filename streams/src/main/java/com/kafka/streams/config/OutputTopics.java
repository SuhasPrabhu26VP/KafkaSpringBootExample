package com.kafka.streams.config;

public final class OutputTopics {

    private OutputTopics() {}


    public static final String STREAM_TABLE_INNER   = "join-stream-table-inner-v1";
    public static final String STREAM_TABLE_LEFT    = "join-stream-table-left-v1";
    public static final String STREAM_TABLE_TEMPORAL = "join-stream-table-temporal-v1";
    public static final String STREAM_STREAM_INNER       = "join-stream-stream-inner-v1";
    public static final String STREAM_STREAM_LEFT        = "join-stream-stream-left-v1";
    public static final String STREAM_STREAM_OUTER       = "join-stream-stream-outer-v1";
    public static final String STREAM_STREAM_ASYMMETRIC  = "join-stream-stream-asymmetric-v1";
    public static final String STREAM_STREAM_GRACE       = "join-stream-stream-grace-v1";
    public static final String TABLE_TABLE_INNER    = "join-table-table-inner-v1";
    public static final String TABLE_TABLE_LEFT     = "join-table-table-left-v1";
    public static final String TABLE_TABLE_FK       = "join-table-table-fk-v1";
    public static final String GLOBAL_INNER         = "join-global-inner-v1";
    public static final String GLOBAL_LEFT          = "join-global-left-v1";
}

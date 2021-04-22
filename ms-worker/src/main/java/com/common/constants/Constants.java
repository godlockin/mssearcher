package com.common.constants;

import com.exception.MsWorkerException;

import java.util.Arrays;
import java.util.List;

public class Constants {
    private Constants() {
        throw new MsWorkerException(ResultEnum.ILLEGAL_METHOD);
    }

    public static class SysConfig {
        private SysConfig() {}
        public static final String KEY_POSITION = "#";
    }

    public static class ESConfig {
        private ESConfig() {}

        public static final int DEFAULT_ES_FROM = 0;
        public static final int DEFAULT_ES_SIZE = 10;
        public static final int DEFAULT_ES_BULK_SIZE = 10;
        public static final int DEFAULT_ES_MAX_SIZE = 10000;
        public static final int DEFAULT_ES_BULK_FLUSH = 5000;
        public static final int DEFAULT_ES_BULK_CONCURRENT = 3;
        public static final int DEFAULT_ES_CONNECT_TIMEOUT = 5000;
        public static final int DEFAULT_ES_SOCKET_TIMEOUT = 40000;
        public static final String DEFAULT_ES_ADDRESS = "localhost:9200";
        public static final String DEFAULT_ES_USERNAME = "";
        public static final String DEFAULT_ES_PASSWORD = "";
        public static final int DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT = 1000;

        public static final String ES_ID = "_es_id";
        public static final String SCORE_KEY = "_score";
        public static final String ES_INDEX = "_es_index";

        public static final String QUERY_KEY = "query";
        public static final String FILTER_KEY = "filter";
        public static final String HIGHLIGHT_KEY = "highlight";
        public static final String AGGREGATION_KEY = "aggregation";
        public static final String FETCH_SOURCE_KEY = "_source";
        public static final String INCLUDES_KEY = "includes";
        public static final String EXCLUDES_KEY = "excludes";
        public static final String SORT_KEY = "sort";

        public static final String NAME_KEY = "name";
        public static final String TYPE_KEY = "type";
        public static final String INDEX_KEY = "index";
        public static final String SIZE_KEY = "size";
        public static final String FIELD_KEY = "field";
        public static final String VALUE_KEY = "value";
        public static final String BOOST_KEY = "boost";

        public static final String MUST_KEY = "must";
        public static final String SHOULD_KEY = "should";
        public static final String MUST_NOT_KEY = "must_not";
        public static final String POST_FILTER_KEY = "post_filter";

        public static final List<String> BOOL_CONDITION_LIST = Arrays.asList(MUST_KEY, SHOULD_KEY, MUST_NOT_KEY);

        public static final String TERM_KEY = "term";
        public static final String TERMS_KEY = "terms";
        public static final String MATCH_KEY = "match";
        public static final String EXISTS_KEY = "exists";
        public static final String FUZZY_KEY = "fuzzy";
        public static final String PREFIX_KEY = "prefix";
        public static final String REGEXP_KEY = "regexp";
        public static final String WRAPPER_KEY = "wrapper";
        public static final String WILDCARD_KEY = "wildcard";
        public static final String QUERY_STRING_KEY = "queryString";
        public static final String MATCH_PHRASE_KEY = "matchPhrase";
        public static final String MATCH_PHRASE_PREFIX_KEY = "matchPhrasePrefix";

        public static final List<String> SIMPLE_CONDITION_LIST = Arrays.asList(TERM_KEY, TERMS_KEY, MATCH_KEY,
                FUZZY_KEY, PREFIX_KEY, REGEXP_KEY, WRAPPER_KEY, WILDCARD_KEY, QUERY_STRING_KEY,
                MATCH_PHRASE_KEY, MATCH_PHRASE_PREFIX_KEY, EXISTS_KEY);

        public static final String RANGE_KEY = "range";

        public static final String INCLUDE_LOWER_KEY = "include_lower";
        public static final String INCLUDE_UPPER_KEY = "include_upper";
        public static final String FROM_KEY = "from";
        public static final String LTE_KEY = "lte";
        public static final String GTE_KEY = "gte";
        public static final String LT_KEY = "lt";
        public static final String GT_KEY = "gt";
        public static final String TO_KEY = "to";

        public static final String MULTIMATCH_KEY = "multiMatch";
        public static final String FIELDNAMES_KEY = "fieldNames";

        public static final String NESTED_KEY = "nested";
        public static final String PATH_KEY = "path";

        public static final String COUNT_KEY = "count";
        public static final String MAX_KEY = "max";
        public static final String MIN_KEY = "min";
        public static final String SUM_KEY = "sum";
        public static final String AVG_KEY = "avg";
        public static final String STATS_KEY = "stats";

        public static final String SHARD_SIZE_KEY = "shardSize";
        public static final String MISSING_KEY = "missing";
        public static final String MIN_DOC_COUNT_KEY = "minDocCount";
        public static final String SHARD_MIN_DOC_COUNT_KEY = "shardMinDocCount";

        public static final List<String> SIMPLE_AGGREGATION_LIST = Arrays.asList(COUNT_KEY, MAX_KEY, MIN_KEY,
                SUM_KEY, AVG_KEY, TERMS_KEY);

        public static final String BOOL_KEY = "bool";

        public static final String COLLAPSE_KEY = "collapse";

        public static final String SUB_AGG_KEY = "subAgg";
        public static final String DATE_RANGE_KEY = "dateRange";

        public static final String ANALYZER_KEY = "analyzer";
        public static final String CHAR_FILTER_KEY = "charFilter";
        public static final String TOKENIZER_KEY = "tokenizer";
        public static final String TOKEN_FILTER_KEY = "tokenFilter";
        public static final String NORMALIZER_KEY = "normalizer";

        public static final String DEFAULT_ANALYZER = "standard";

        public static final String SCROLL_TIME_VALUE_KEY = "timeValue";
        public static final String DEFAULT_SCROLL_TIME_VALUE = "1h";

        public static final String ORDER_KEY = "order";
        public static final String SORT_ORDER_ASC = "asc";
        public static final String SORT_ORDER_DESC = "desc";
        public static final String FIELD_SORT_TYPE = "field";
        public static final String SCRIPT_SORT_TYPE = "script";
        public static final String SCRIPT_TYPE = "scriptType";
        public static final String SCRIPT_LANG = "scriptLang";
        public static final String SCRIPT_SORT_SCRIPT_TYPE = "scriptSortType";
        public static final String SORT_MODE = "sortMode";
        public static final String SCRIPT_OPTIONS = "scriptOptions";
        public static final String SCRIPT_PARAMS = "scriptParams";

        public static final String NUMBER_TYPE = "number";
        public static final String STRING_TYPE = "string";

        public static final String MIN_MODE = "min";
        public static final String MAX_MODE = "max";
        public static final String SUM_MODE = "sum";
        public static final String AVG_MODE = "avg";
        public static final String MEDIAN_MODE = "median";

        public static final String INLINE_TYPE = "inline";
        public static final String STORED_TYPE = "stored";

        public static final String PAINLESS_TYPE = "painless";
    }

    public static class RedisConfig {
        private RedisConfig() {}

        public static final Integer DEFAULT_REDIS_PROTOCOL_TIMEOUT = 2000;
        public static final Integer DEFAULT_REDIS_MAX_IDLE = 3;
        public static final Integer DEFAULT_REDIS_MAX_TOTAL = 1000;
        public static final Integer DEFAULT_REDIS_MAX_WAIT_MILLIS = 10;

        public static final Long DEFAULT_REDIS_MIN_EVICTABLE_IDLE_TIME_MILLIS = 1000L;
        public static final Integer DEFAULT_REDIS_NUM_TESTS_PER_EVICTION_RUN = 3;
        public static final Integer DEFAULT_REDIS_TIME_BETWEEN_EVICTION_RUNS_MILLIS = 3;
        public static final Boolean DEFAULT_REDIS_TEST_ON_BORROW = true;
        public static final Boolean DEFAULT_REDIS_TEST_WHILE_IDLE = true;

        public static final String DEFAULT_REDIS_HOST = "localhost";
        public static final int DEFAULT_REDIS_PORT = 6379;
        public static final String DEFAULT_REDIS_PASSWORD = "9a46259f1b75feaa";
        public static final int DEFAULT_REDIS_DATABASE = 0;
    }

    public static class ResultConfig {
        private ResultConfig() {}

        public static final String DATA_KEY = "data";
        public static final String TOTAL_KEY = "total";
        public static final String HIGHLIGHT_KEY = "highlight";
        public static final String AGGREGATION_KEY = "aggregation";
        public static final String SCROLL_ID_KEY = "scrollId";
    }
}


-type partition_server() :: atom().
-type pref() :: {partition_server(), partition_id()}.

-type partition_id() :: non_neg_integer().
-type tx_id() :: term().
-type key() :: term().
-type val() :: term().
-type ws() :: pvc_writeset:ws(key(), val()).
-type vc() :: pvc_vclock:vc(partition_id()).

-type read_partitions() :: ordsets:ordset(partition_id()).
-type abort() :: {error, reason()}.
-type reason() :: partition_not_started
                | abort_read
                | abort_stale
                | abort_conflict
                | try_again.
-type outcome() :: {ok, vc()} | abort.

-define(VERSION_THRESHOLD, 500).
-define(MAX_VERSIONS, 100).
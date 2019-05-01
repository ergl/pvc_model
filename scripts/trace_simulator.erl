Ring = pvc_model:start_partitions([0,1,2]).
Client = pvc_client:new(Ring).

Prefix = [
  % Transaction A reads and updates, inits prepare
  #{id => tx_a,op => start_tx},
  #{id => tx_a,op => {read,[2]}},
  #{id => tx_a,op => {update,[{2,0}]}},
  #{id => tx_a,op => prepare},

  % Transaction B, reads and updates, but does not start commit
  #{id => tx_b,op => start_tx},
  #{id => tx_b,op => {read,[0,1]}},
  #{id => tx_b,op => {update,[{0,0},{1,0}]}},

  % Transaction C runs to completion
  #{id => tx_c,op => start_tx},
  #{id => tx_c,op => {read,[1,2]}},
  #{id => tx_c,op => {update,[{4,0},{5,0}]}}, %% 4 and 5 fall in 1 and 2
  #{id => tx_c,op => prepare},
  #{id => tx_c,op => commit},

  % Transaction B commits here
  #{id => tx_b,op => prepare},
  #{id => tx_b,op => commit}].

[] = history_executor:materialize_execution(Prefix, Client).

Critical = [#{id => tx_d,op => start_tx},
            #{id => tx_d,op => {read,[0,2,1]}}].

history_executor:materialize_execution(Critical, Client).

%% Result: [{tx_d,{read,[0,2,1]},{error,abort_read}}]

pvc_model:stop_partitions(Ring), f().

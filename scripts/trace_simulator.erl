Ring = pvc_model:start_partitions([0,1,2,3,4,5,6,7]).
Client = pvc_client:new(Ring).

Prefix = [
  % Transaction A
  #{id => tx_a,op => start_tx},
  #{id => tx_a,op => {read,[4]}},
  #{id => tx_a,op => {update,[{4,0}]}},
  #{id => tx_a,op => prepare},
  #{id => tx_a,op => commit},

  % Transaction B, reads and updates, but does not start commit
  #{id => tx_b,op => start_tx},
  #{id => tx_b,op => {read,[0,4]}},
  #{id => tx_b,op => {update,[{0,0},{4,0}]}},

  % Transaction C reads and updates, inits prepare
  #{id => tx_c,op => start_tx},
  #{id => tx_c,op => {read,[1]}},
  #{id => tx_c,op => {update,[{1,0}]}},
  #{id => tx_c,op => prepare},

  % Transaction D runs entirely here
  #{id => tx_d,op => start_tx},
  #{id => tx_d,op => {read,[1,4]}},
  #{id => tx_d,op => {update,[{9,0},{12,0}]}}, %% 9 and 12 fall in 1 and 4
  #{id => tx_d,op => prepare},
  #{id => tx_d,op => commit},

  % Transaction B commits here
  #{id => tx_b,op => prepare},
  #{id => tx_b,op => commit}].

[] = history_executor:materialize_execution(Prefix, Client).

Critical = [#{id => tx_e,op => start_tx},
            #{id => tx_e,op => {read,[0,1,4]}}].

history_executor:materialize_execution(Critical, Client).

%% Result: [{tx_e,{read,[0,1,4]},{error,abort_read}}]

pvc_model:stop_partitions(Ring), f().
# `configure new` is not idempotent: fdbcli refuses it once the database
# exists. deploy_stack.sh configures the database itself -- it has to, because
# the services it waits for cannot start without one -- and
# cluster_ops.create_cluster still calls this script afterwards. That second
# call has to be a no-op rather than an error.
if fdbcli --exec 'status minimal' --timeout 20 -C /etc/foundationdb/fdb.cluster 2>&1 \
        | grep -q 'The database is available'; then
    echo "Database already configured"
    exit 0
fi

fdbcli --exec "configure new single ssd" --timeout 100 -C /etc/foundationdb/fdb.cluster
sleep 10

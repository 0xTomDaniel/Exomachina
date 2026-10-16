#!/bin/sh
# Isolated Kestra OSS 2.0.3 + PostgreSQL 16.15 trial launcher.
set -eu
: "${EXO_KESTRA_BUNDLE:?point to the pinned S0 local bundle}"
: "${EXO_KESTRA_STATE:?point to a new state directory outside the repository}"
: "${EXO_KESTRA_PG_PORT:?set a free local PostgreSQL port}"
: "${EXO_KESTRA_HTTP_PORT:?set a free local HTTP port}"
bundle_root=$EXO_KESTRA_BUNDLE
data_dir=$EXO_KESTRA_STATE
pg_bin="$bundle_root/pg/bin"
java_bin="$bundle_root/runtime/Contents/Home/bin/java"
kestra_jar="$bundle_root/runtime/kestra-2.0.3"
pg_dir="$data_dir/postgres"
socket_dir="$data_dir/socket"
PATH=/usr/bin:/bin:/usr/sbin:/sbin
LC_ALL=C.UTF-8
export PATH LC_ALL
umask 077
mkdir -p "$data_dir/storage" "$socket_dir"
if [ ! -f "$data_dir/pg.pass" ]; then
  /usr/bin/openssl rand -hex 32 > "$data_dir/pg.pass"
fi
if [ ! -f "$data_dir/http.pass" ]; then
  printf 'Aa1%s\n' "$(/usr/bin/openssl rand -hex 32)" > "$data_dir/http.pass"
fi
pg_password=$(cat "$data_dir/pg.pass")
http_password=$(cat "$data_dir/http.pass")
pg_user=$(/usr/bin/id -un)
if [ ! -f "$pg_dir/PG_VERSION" ]; then
  "$pg_bin/initdb" -D "$pg_dir" --auth-local=scram-sha-256 --auth-host=scram-sha-256 --pwfile="$data_dir/pg.pass" > "$data_dir/initdb.log" 2>&1
fi
pg_options="-p $EXO_KESTRA_PG_PORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$socket_dir"
"$pg_bin/pg_ctl" -D "$pg_dir" -l "$data_dir/postgres.log" -o "$pg_options" start > "$data_dir/pg-start.log" 2>&1
export PGPASSWORD="$pg_password"
if ! "$pg_bin/psql" -h 127.0.0.1 -p "$EXO_KESTRA_PG_PORT" -U "$pg_user" -d postgres -Atqc "select 1 from pg_database where datname='kestra'" | /usr/bin/grep -q '^1$'; then
  "$pg_bin/createdb" -h 127.0.0.1 -p "$EXO_KESTRA_PG_PORT" -U "$pg_user" kestra
fi
unset PGPASSWORD
cat > "$data_dir/config.yml" <<EOF
micronaut:
  server:
    host: 127.0.0.1
kestra:
  allocated-cpu-cores: 2
  server:
    basic-auth:
      username: parity@example.invalid
      password: $http_password
  repository:
    type: postgres
  queue:
    type: postgres
  storage:
    type: local
    local:
      base-path: $data_dir/storage
datasources:
  postgres:
    url: jdbc:postgresql://127.0.0.1:$EXO_KESTRA_PG_PORT/kestra
    driver-class-name: org.postgresql.Driver
    username: $pg_user
    password: $pg_password
EOF
cat > "$data_dir/api.netrc" <<EOF
machine 127.0.0.1 login parity@example.invalid password $http_password
EOF
"$java_bin" -Xms256m -Xmx1g -jar "$kestra_jar" server standalone -c "$data_dir/config.yml" --port="$EXO_KESTRA_HTTP_PORT" --worker-thread=4 --no-tutorials > "$data_dir/kestra.log" 2>&1 &
kestra_pid=$!
cleanup() {
  kill "$kestra_pid" 2>/dev/null || true
  wait "$kestra_pid" 2>/dev/null || true
  "$pg_bin/pg_ctl" -D "$pg_dir" stop -m fast > "$data_dir/pg-stop.log" 2>&1 || true
}
trap cleanup EXIT
trap 'exit 0' INT TERM
wait "$kestra_pid"

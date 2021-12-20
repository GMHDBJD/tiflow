#!/bin/bash

set -eu

cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source $cur/../_utils/test_prepare
WORK_DIR=$TEST_DIR/$TEST_NAME
source $cur/../_utils/shardddl_lib.sh

function DM_DIFFERENT_SCHEMA_FULL_CASE() {
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb}" "count(1): 4"
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(6,'6');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,'77');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,'8','88');"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c text;"
	# source1.tb1(a,c); source1.tb2(a,b); source2.tb1(a,c); source2.tb2(a,b,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(9,'999');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(10,'1010');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,'111111');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,'1212','121212');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	# source1.tb1(a,c); source1.tb2(a,b); source2.tb1(a,c); source2.tb2(a,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,'131313');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(14,'1414');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,'151515');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,'161616');"

	run_sql_source1 "alter table ${shardddl1}.${tb2} drop column b;"
	# source1.tb1(a,c); source1.tb2(a); source2.tb1(a,c); source2.tb2(a,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,'171717');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(18);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(19,'191919');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(20,'202020');"

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column c text;"
	# source1.tb1(a,c); source1.tb2(a,c); source2.tb1(a,c); source2.tb2(a,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,'212121');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(22,'222222');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(23,'232323');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(24,'242424');"

	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb}" "count(1): 24"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DIFFERENT_SCHEMA_FULL() {
	# create table with different schema, init data, and create table in downstream manually
	run_case DIFFERENT_SCHEMA_FULL "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key);\"; \
     run_sql_source1 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c text);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10), c text);\"; \
     run_sql_source1 \"insert into ${shardddl1}.${tb1} values(1);\"; \
     run_sql_source1 \"insert into ${shardddl1}.${tb2} values(2,'22');\"; \
     run_sql_source2 \"insert into ${shardddl1}.${tb1} values(3,'333');\"; \
     run_sql_source2 \"insert into ${shardddl1}.${tb2} values(4,'44','444');\"; \
     run_sql_tidb \"create database if not exists ${shardddl};\"; \
     run_sql_tidb \"create table ${shardddl}.${tb} (a int primary key, b varchar(10), c text);\"" \
		"clean_table" "optimistic"
}

function DM_DIFFERENT_SCHEMA_INCREMENTAL_CASE() {
	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb}" "count(1): 4"

	# get checkpoint
	source1_status=($(get_master_status $MYSQL_HOST1 $MYSQL_PORT1))
	source2_status=($(get_master_status $MYSQL_HOST2 $MYSQL_PORT2))

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"pause-task test" \
		"\"result\": true" 3

	# save schema
	curl -X GET http://127.0.0.1:8261/api/v1/tasks/test/sources/mysql-replica-01/schemas/${shardddl1}/${tb1} | jq -r .schema_create_sql > $WORK_DIR/schema11.sql
	curl -X GET http://127.0.0.1:8261/api/v1/tasks/test/sources/mysql-replica-01/schemas/${shardddl1}/${tb2} | jq -r .schema_create_sql > $WORK_DIR/schema12.sql
	curl -X GET http://127.0.0.1:8261/api/v1/tasks/test/sources/mysql-replica-02/schemas/${shardddl1}/${tb1} | jq -r .schema_create_sql > $WORK_DIR/schema21.sql
	curl -X GET http://127.0.0.1:8261/api/v1/tasks/test/sources/mysql-replica-02/schemas/${shardddl1}/${tb2} | jq -r .schema_create_sql > $WORK_DIR/schema22.sql

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"stop-task test" \
		"\"result\": true" 3

	# incremental data
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(5);"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(6,'6');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(7,'77');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(8,'8','88');"

	run_sql_source1 "alter table ${shardddl1}.${tb1} add column c text;"
	# source1.tb1(a,c); source1.tb2(a,b); source2.tb1(a,c); source2.tb2(a,b,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(9,'999');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(10,'1010');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(11,'111111');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(12,'1212','121212');"

	run_sql_source2 "alter table ${shardddl1}.${tb2} drop column b;"
	# source1.tb1(a,c); source1.tb2(a,b); source2.tb1(a,c); source2.tb2(a,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(13,'131313');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(14,'1414');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(15,'151515');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(16,'161616');"

	run_sql_source1 "alter table ${shardddl1}.${tb2} drop column b;"
	# source1.tb1(a,c); source1.tb2(a); source2.tb1(a,c); source2.tb2(a,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(17,'171717');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(18);"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(19,'191919');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(20,'202020');"

	run_sql_source1 "alter table ${shardddl1}.${tb2} add column c text;"
	# source1.tb1(a,c); source1.tb2(a,c); source2.tb1(a,c); source2.tb2(a,c)
	run_sql_source1 "insert into ${shardddl1}.${tb1} values(21,'212121');"
	run_sql_source1 "insert into ${shardddl1}.${tb2} values(22,'222222');"
	run_sql_source2 "insert into ${shardddl1}.${tb1} values(23,'232323');"
	run_sql_source2 "insert into ${shardddl1}.${tb2} values(24,'242424');"

	# start task with current checkpoint	
	sed "s/pos-holder/${source1_status[1]}/g" $cur/conf/double-source-optimistic-incr.yaml >$WORK_DIR/task.yaml
	sed -i "s/name-holder/${source1_status[0]}/g" $WORK_DIR/task.yaml
	sed -i "s/gtid-holder/${source2_status[2]}/g" $WORK_DIR/task.yaml
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"start-task $WORK_DIR/task.yaml --remove-meta"

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"query-status test" \
		"Column count doesn't match" 2

	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-01 test -d ${shardddl1} -t ${tb1} $WORK_DIR/schema11.sql" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-01 test -d ${shardddl1} -t ${tb2} $WORK_DIR/schema12.sql" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-02 test -d ${shardddl1} -t ${tb1} $WORK_DIR/schema21.sql" \
		"\"result\": true" 2
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"operate-schema set -s mysql-replica-02 test -d ${shardddl1} -t ${tb2} $WORK_DIR/schema22.sql" \
		"\"result\": true" 2
	
	run_dm_ctl $WORK_DIR "127.0.0.1:$MASTER_PORT" \
		"resume-task test" \
		"\"result\": true" 3

	run_sql_tidb_with_retry "select count(1) from ${shardddl}.${tb}" "count(1): 24"
	check_sync_diff $WORK_DIR $cur/conf/diff_config.toml
}

function DM_DIFFERENT_SCHEMA_INCREMENTAL() {
	# create table with different schema, init data, and create table in downstream manually
	run_case DIFFERENT_SCHEMA_INCREMENTAL "double-source-optimistic" \
		"run_sql_source1 \"create table ${shardddl1}.${tb1} (a int primary key);\"; \
     run_sql_source1 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10));\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb1} (a int primary key, c text);\"; \
     run_sql_source2 \"create table ${shardddl1}.${tb2} (a int primary key, b varchar(10), c text);\"; \
     run_sql_source1 \"insert into ${shardddl1}.${tb1} values(1);\"; \
     run_sql_source1 \"insert into ${shardddl1}.${tb2} values(2,'22');\"; \
     run_sql_source2 \"insert into ${shardddl1}.${tb1} values(3,'333');\"; \
     run_sql_source2 \"insert into ${shardddl1}.${tb2} values(4,'44','444');\"; \
     run_sql_tidb \"create database if not exists ${shardddl};\"; \
     run_sql_tidb \"create table ${shardddl}.${tb} (a int primary key, b varchar(10), c text);\"" \
		"clean_table" "optimistic"
}

function run() {
	init_cluster
	init_database

	DM_DIFFERENT_SCHEMA_FULL
	DM_DIFFERENT_SCHEMA_INCREMENTAL
}

cleanup_data $shardddl
cleanup_data $shardddl1
cleanup_data $shardddl2
# also cleanup dm processes in case of last run failed
cleanup_process $*
run $*
cleanup_process $*

echo "[$(date)] <<<<<< test case $TEST_NAME success! >>>>>>"

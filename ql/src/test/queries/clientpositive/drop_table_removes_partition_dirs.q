--! qt:dataset:src
--! qt:dataset:part
-- This test verifies that if a partition exists outside the table's current location when the
-- table is dropped the partition's location is dropped as well.

CREATE TABLE test_table_n3 (key STRING, value STRING)
PARTITIONED BY (part STRING)
STORED AS RCFILE
LOCATION 'file:${system:test.tmp.dir}/drop_table_removes_partition_dirs_table';

ALTER TABLE test_table_n3 ADD PARTITION (part = '1')
LOCATION 'file:${system:test.tmp.dir}/drop_table_removes_partition_dirs_table2/part=1';

INSERT OVERWRITE TABLE test_table_n3 PARTITION (part = '1')
SELECT * FROM src;

dfs -ls ${system:test.tmp.dir}/drop_table_removes_partition_dirs_table2;

DROP TABLE test_table_n3;

dfs -ls ${system:test.tmp.dir}/drop_table_removes_partition_dirs_table2;

dfs -rm -r -f ${system:test.tmp.dir}/drop_table_removes_partition_dirs_table2;

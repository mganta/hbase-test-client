hbase-test-client
=================

This example utility acts as an hbase client and does the following

1. Creates an HBase table pre-split into regions (drops the table if already exists)
2. Spawns a set of threads and loads data into the table
3. The table will have 100 columns and the data is spread into 26 regions. 



Master template for the program from https://github.com/yujikosuga/hbase-java-sample

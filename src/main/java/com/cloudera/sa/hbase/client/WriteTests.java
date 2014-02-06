package com.cloudera.sa.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;

public class WriteTests {
	
    private static boolean INITIALIZE_AT_FIRST = true;
    private static int THREAD_COUNT = 20;
    private static int REGION_COUNT = 26;
    private static int HBASE_POOL_COUNT = 8;
    private static String TABLE_NAME="Table1";
    private static String COLUMN_FAMILY="family1";
    private static HTablePool pool;

    private void createTable(HBaseAdmin admin) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(TABLE_NAME.getBytes());
        HColumnDescriptor cDesc = new HColumnDescriptor(COLUMN_FAMILY.getBytes());
        cDesc.setCompressionType(Algorithm.SNAPPY);
        desc.addFamily(cDesc);
        admin.createTable(desc, "a".getBytes(), "z".getBytes(), REGION_COUNT);
    }

    private void deleteTable(HBaseAdmin admin) throws IOException {
        if (admin.tableExists(TABLE_NAME.getBytes())) {
            admin.disableTable(TABLE_NAME.getBytes());
            try {
                admin.deleteTable(TABLE_NAME.getBytes());
            } finally {
            }
        }
    }

  
    public void run(Configuration config) throws IOException {
       	
        HBaseAdmin admin = new HBaseAdmin(config);

        if (INITIALIZE_AT_FIRST) {
        	System.out.println("Dropping Table " + TABLE_NAME);
            deleteTable(admin);
        }

        if (!admin.tableExists(TABLE_NAME.getBytes())) {
            createTable(admin);
            System.out.println("Created Table " + TABLE_NAME);
        }

         pool = new HTablePool(config, HBASE_POOL_COUNT);
         
         for(int i=0; i < THREAD_COUNT; i++) {
        	 WorkerThread t = new WorkerThread(pool, i, TABLE_NAME, COLUMN_FAMILY);
        	 t.start();
         }

       pool.close();
       admin.close();
    }
}
package com.cloudera.sa.hbase.client;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.PoolMap;

public class WorkerThread extends Thread {
	
	private static int RECORD_COUNT=50;
	private static int COLUMN_COUNT=100;
	private static int COLUMN_BYTES=100;
	private static int REGION_COUNT = 26;

        private static int HBASE_POOL_COUNT = 1;
        private static int PRINT_INFO_COUNT = 10;

	private int threadId;
	private String COLUMN_FAMILY;

	private final String[] qualifiers;
	private final byte[][] values;
	private String prefix = "abcdefghijklmnopqrstuvwxyz";
	private HTableInterface table;
        private HTablePool pool;

	public WorkerThread(Configuration config, int threadId, String tableName, String columnName) {
		this.pool = new HTablePool(config, HBASE_POOL_COUNT, PoolMap.PoolType.ThreadLocal);
		this.table = pool.getTable(tableName);
		this.threadId = threadId;
		COLUMN_FAMILY=columnName;
		
		qualifiers = new String[COLUMN_COUNT];
		values = new byte[COLUMN_COUNT][COLUMN_BYTES];
		byte[] b = new byte[COLUMN_BYTES];

		for (int i = 0; i < COLUMN_COUNT; i++) {
			qualifiers[i] = new String("Column_" + i);
			new Random().nextBytes(b);
			values[i] = b;
		}
	}

	@Override
	public void run() {
		try {
	         long startTime = System.currentTimeMillis();
	         System.out.println("Starting Load from thread " + threadId);
			int prefixIndex = 0;
			for (int i = 0; i < RECORD_COUNT; i++) {
				if (i % PRINT_INFO_COUNT == 0)
				  System.out.println( i + " records so far from thread " + threadId );
				if (prefixIndex == REGION_COUNT -1)
					prefixIndex = 0;
				Put p = new Put((prefix.charAt(prefixIndex) + UUID.randomUUID()
						.toString()).getBytes());
				for (int j = 0; j < COLUMN_COUNT; j++)
					p.add(COLUMN_FAMILY.getBytes(), qualifiers[j].getBytes(), values[j]);
				table.put(p);
				prefixIndex++;
			}
			 long endTime = System.currentTimeMillis();
	         System.out.println("Total time taken by Thread-" + threadId + " = "  + (endTime - startTime) + " milliseconds");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

}

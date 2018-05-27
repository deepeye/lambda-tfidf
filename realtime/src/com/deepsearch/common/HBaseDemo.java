package com.deepsearch.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class HBaseDemo {

	private static final Logger logger = LoggerFactory.getLogger(HBaseDemo.class);  
	
	private static Configuration config = null;
    private static Connection connection = null;
	
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "localhost");
	}
	
	/*获得Connection连接*/
    public static Connection getConnecton(){
        if(connection == null || connection.isClosed()){
            try {
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
            	e.printStackTrace();
            	logger.error("get connection failure !", e);
            }
        }
        return connection;
    }
    
    /**
	 * 关闭连接
	 */
	public static void close() {
		try {
			if (null != connection) {
				connection.close();
			}
		} catch (IOException e) {
			logger.error("close connection failure !", e);
		}
	}
	
	/** 
     * 获取  Table 
     * @param tableName 表名 
     * @return 
     * @throws IOException 
     */  
    public static Table getTable(String tableName){  
        try {  
            return getConnecton().getTable(TableName.valueOf(tableName));  
        } catch (Exception e) {  
            logger.error("Obtain Table failure !", e);  
        }  
        return null;  
    }  
    
	/**
	 * 批量写入
	 * */
	public static void pubBatch(String tableName,List<Tuple2<String, Integer>> output,String columnFamily, String qualifier){
		if(output == null || output.size()==0) return;
		try {
			Table table = getConnecton().getTable(TableName.valueOf(tableName));
			List<Put> puts = new ArrayList<Put>(output.size());
			for(Tuple2<?,?> tuple : output){
				Put put = new Put(Bytes.toBytes(tuple._1().toString()));
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), tuple._2().toString().getBytes());
				puts.add(put);
			}
			table.put(puts);
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("put batch failure !", e);
		} finally{
			close();
		}
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		List<Tuple2<String, Integer>> output = new ArrayList<Tuple2<String, Integer>>();
		output.addAll(Arrays.asList(new Tuple2<>("word1",30),
				new Tuple2<>("word2",2),
				new Tuple2<>("word3",22)));
		HBaseDemo.pubBatch("d_table", output, "cf", "df");
	}

}

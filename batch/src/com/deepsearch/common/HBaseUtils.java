package com.deepsearch.common;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class HBaseUtils {

	private static final Logger logger = LoggerFactory.getLogger(HBaseUtils.class);  
	
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
	
	/*
     * 将byte[]转换成String
     * UTF8编码
     */
    public static String changeByteToString(byte[] b) throws UnsupportedEncodingException{
        String ret = null;
        if(b.length > 0){
            ret = new String(b,"UTF8");
        }
        return ret;
    }
    
    /**
     * 删除记录
     * */
    public static void delRecord(String tableName,String rowKey){
    		Table table = null;
        try{
            table = getTable(tableName);
            List<Delete> list = new ArrayList<Delete>();
            Delete delete = new Delete(rowKey.getBytes());
            list.add(delete);
            table.delete(list);
            System.out.println("删除记录：rowKey【" + rowKey + "】从表【" + tableName + "】成功");
        }catch(IOException e){
            System.out.println("删除记录：rowKey【" + rowKey + "】从表【" + tableName + "】失败" + e.getMessage());
        }
    }
    
    /**
     * 获取记录
     * */
    public static String getRecordByRowKey(String tableName,String rowKey){
        try {
            Table table = getTable(tableName);
            Get get = new Get(rowKey.getBytes());
            Result rs = table.get(get);
            StringBuilder sb = new StringBuilder();
            for(Cell cell : rs.rawCells()){
            	sb.append("RowName:"+new String(CellUtil.cloneRow(cell))+"\t");
            	sb.append("Timetamp:"+cell.getTimestamp()+"\t");
            	sb.append("column Family:"+new String(CellUtil.cloneFamily(cell))+"\t");
            	sb.append("row Name:"+new String(CellUtil.cloneQualifier(cell))+"\t");
            	sb.append("value:"+new String(CellUtil.cloneValue(cell))+"\t");
            	sb.append("\n");
            }
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
     
    /**
     * 获取所有记录
     * */
    public static String getAllRecord(String tableName){
        try{
            Table table = getTable(tableName);
            Scan scan = new Scan();
            ResultScanner rscan = table.getScanner(scan);
            StringBuilder sb = new StringBuilder();
            for(Result rs : rscan){
            	for(Cell cell : rs.rawCells()){
                /*	sb.append("RowName:"+new String(CellUtil.cloneRow(cell))+"\t");
                	sb.append("Timetamp:"+cell.getTimestamp()+"\t");
                	sb.append("column Family:"+new String(CellUtil.cloneFamily(cell))+"\t");
                	sb.append("row Name:"+new String(CellUtil.cloneQualifier(cell))+"\t");
                	sb.append("value:"+new String(CellUtil.cloneValue(cell))+"");*/
                	
                	sb.append(new String(CellUtil.cloneRow(cell))+"\t");
                	sb.append(new String(CellUtil.cloneValue(cell))+"");
                	sb.append("\n");
                }
            }
            return sb.toString();
        }catch(IOException e){
            System.out.println("结果为【null】 " + e.getMessage());
        }
        return null;
    }
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		
		/*List<Tuple2<String, Integer>> output = new ArrayList<Tuple2<String, Integer>>();
		output.addAll(Arrays.asList(new Tuple2<>("word4",30),
				new Tuple2<>("word5",2),
				new Tuple2<>("word6",22)));
		HBaseUtils.pubBatch("d_table", output, "cf", "df");*/
		
		String result = HBaseUtils.getAllRecord("tf_rt");
		System.out.println(result);
//		File file = new File("/Users/walter/Documents/jianguo/workspace/sparkdemo/files/tf_count.txt");
//		FileUtils.write(file, result, "utf8");
	}

}

package com.deepsearch;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.deepsearch.functions.BatchCombiner;
import com.deepsearch.functions.DocumentTokenizer;
import com.deepsearch.functions.SplitAndProjectToFields;
import com.deepsearch.functions.StaticSourceFunction;
import com.deepsearch.functions.TfidfExpression;
import com.deepsearch.spouts.DocumentSpout;
import com.deepsearch.trident.mapper.SimpleTridentHBaseMapMapper;
import com.deepsearch.trident.mapper.TridentHBaseMapMapper;
import com.deepsearch.trident.state.HBaseMapState;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.spout.ITridentSpout;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.Split;

public class TermTopology {

	static Logger LOG = LoggerFactory.getLogger(TermTopology.class);
	
	private static StateFactory getStateFactory(String tableName) {
		HBaseMapState.Options options = new HBaseMapState.Options();
		options.tableName = tableName;
		options.columnFamily = "cf";
		String qualifier = "count";
		TridentHBaseMapMapper mapMapper = new SimpleTridentHBaseMapMapper(qualifier, true);
		options.mapMapper = mapMapper;
		return HBaseMapState.nonTransactional(options);
	}
	
	private static StateFactory getBatchStateFactory(String tableName) {
		HBaseMapState.Options options = new HBaseMapState.Options();
		options.tableName = tableName;
		options.columnFamily = "cf";
		String qualifier = "count";
		TridentHBaseMapMapper mapMapper = new SimpleTridentHBaseMapMapper(qualifier, false);
		options.mapMapper = mapMapper;
		return HBaseMapState.nonTransactional(options);
	}
	/**
	 * get document stream
	 * */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Stream getDocumentStream(TridentTopology topology, ITridentSpout spout){
		Stream stream = null;
		if(spout == null){	
			FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("document","documentId","source"), 1,
					new Values("我是中国人","doc50001", "article"),
					new Values("西湖风景区，G20峰会在杭州召开","doc50002","article"),
					new Values("习近平出招破解全球经济疲软","doc50003","article"),
					new Values("Chinese official stopped Obama's senior defence advisor from boarding the US president's car","doc50004","article"),
					new Values("iphone7来了！赶紧先睹为快","doc50005","article"),
					new Values("东边日出西边雨水，雨水很大，尤其在夏天，地面上的雨水成河","doc50006", "article")
					);
			testSpout.setCycle(false);
			stream = topology.newStream("spout1", testSpout);
		}else{
			stream = topology.newStream("spout1", spout);
		}
		return stream;
	}
	
	private static void addDFQueryStream(TridentState dfState,
			TridentTopology topology, LocalDRPC drpc){
		topology.newDRPCStream("dfQuery", drpc)
			.each(new Fields("args"), new Split(), new Fields("term"))
			.stateQuery(dfState, new Fields("term"), new MapGet(), new Fields("df"))
			.each(new Fields("df"), new FilterNull())
			.project(new Fields("term","df"));
	}
	
	private static void addDQueryStream(TridentState dState,
			TridentTopology topology, LocalDRPC drpc){
		topology.newDRPCStream("dQuery", drpc)
		.each(new Fields("args"), new Split(), new Fields("source"))
		.stateQuery(dState, new Fields("source"), new MapGet(), new Fields("d"))
		.each(new Fields("d"), new FilterNull())
		.project(new Fields("source","d"));
	}
	
	private static void addTFIDFQueryStream(TridentState tfState,
			TridentState dfState,
			TridentState dState,
			TridentTopology topology, LocalDRPC drpc) {
		TridentState batchDfState = topology.newStaticState(getBatchStateFactory("df_batch"));
		TridentState batchDState = topology.newStaticState(getBatchStateFactory("d_batch"));
		TridentState batchTfState = topology.newStaticState(getBatchStateFactory("tf_batch"));
		
		topology.newDRPCStream("tfidfQuery",drpc)
			.each(new Fields("args"), new SplitAndProjectToFields(), new Fields("documentId", "term"))
			.each(new Fields(), new StaticSourceFunction("article"), new Fields("source"))
			.stateQuery(tfState, new Fields("documentId", "term"), new MapGet(), new Fields("tf_rt"))
			.stateQuery(dfState,new Fields("term"), new MapGet(), new Fields("df_rt"))
			.stateQuery(dState,new Fields("source"), new MapGet(), new Fields("d_rt"))
			.stateQuery(batchTfState, new Fields("documentId", "term"), new MapGet(), new Fields("tf_batch"))
			.stateQuery(batchDfState,new Fields("term"), new MapGet(), new Fields("df_batch"))
			.stateQuery(batchDState,new Fields("source"), new MapGet(), new Fields("d_batch"))
			.each(new Fields("tf_rt", "df_rt","d_rt","tf_batch","df_batch","d_batch"), new BatchCombiner(), new Fields("tf","d","df"))
			.each(new Fields("term","documentId","tf","d","df"), new TfidfExpression(), new Fields("tfidf"))
			.each(new Fields("tfidf"), new FilterNull())
			.project(new Fields("documentId","term","tfidf"));
		
	}
	
	
	/**
	 * build test topology
	 * */
	@SuppressWarnings("rawtypes")
	public static TridentTopology buildTestTopology(ITridentSpout spout, LocalDRPC drpc){
		TridentTopology topology = new TridentTopology();
		
		Stream documentStream = getDocumentStream(topology, spout);
		
		Stream termStream = documentStream
				.parallelismHint(20)
				.each(new Fields("document"), new DocumentTokenizer(), new Fields("term"))
				.project(new Fields("term","documentId","source"));
		
		TridentState dfState = termStream.groupBy(new Fields("term"))
				.persistentAggregate(getBatchStateFactory("df_batch"),	new Count(), new Fields("df"));
		
		//create d state
		TridentState dState = documentStream.groupBy(new Fields("source"))
				.persistentAggregate(getBatchStateFactory("d_batch"), new Count(), new Fields("d"));
				
		TridentState tfState = termStream.groupBy(new Fields("documentId", "term"))
				.persistentAggregate(getBatchStateFactory("tf_batch"), new Count(), new Fields("tf"));
				
		/*topology.newDRPCStream("df", drpc)
			.each(new Fields("args"), new SplitWord(),new Fields("term"))
			.stateQuery(dfState, new Fields("term"), new MapGet(), new Fields("df"))
			.each(new Fields("df"), new FilterNull())
			.project(new Fields("term","df"));*/
		
		return topology;
	}
	
	/**
	 * build topology
	 * */
	@SuppressWarnings("rawtypes")
	public static TridentTopology buildTopology(ITridentSpout spout, LocalDRPC drpc){
		TridentTopology topology = new TridentTopology();
		
		//get doucment stream
		Stream documentStream = getDocumentStream(topology, spout);
		
		//get term stream
		Stream termStream = documentStream
				.parallelismHint(1)
				.each(new Fields("document"), new DocumentTokenizer(), new Fields("term"))
				.project(new Fields("term","documentId","source"));
		
		//create df state
		TridentState dfState = termStream.groupBy(new Fields("term"))
				.persistentAggregate(getStateFactory("df_rt"), new Count(), new Fields("df"));
		//create df drpc stream
		addDFQueryStream(dfState, topology, drpc);
		
		//create d state
		TridentState dState = documentStream.groupBy(new Fields("source"))
				.persistentAggregate(getStateFactory("d_rt"), new Count(), new Fields("d"));
		//create d stream
		addDQueryStream(dState, topology, drpc);
		
		//create tfState
		TridentState tfState = termStream.groupBy(new Fields("documentId", "term"))
				.persistentAggregate(getStateFactory("tf_rt"), new Count(), new Fields("tf"));
		
		addTFIDFQueryStream(tfState, dfState, dState, topology, drpc);
		
		return topology;
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub

		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		
		/*if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			conf.setDebug(false);
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("hbase.zookeeper.property.clientPort", "2181");
			map.put("hbase.zookeeper.quorum", "localhost");
			conf.put("hbase.config", map);
			TridentTopology topology = buildTestTopology(null,drpc);
			cluster.submitTopology("tfidf", conf, topology.build());
		}*/
		
		if (args.length == 0) {
			LocalDRPC drpc = new LocalDRPC();
			LocalCluster cluster = new LocalCluster();
			conf.setDebug(true);
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("hbase.zookeeper.property.clientPort", "2181");
			map.put("hbase.zookeeper.quorum", "localhost");
			conf.put("hbase.config", map);
			
			TridentTopology topology = buildTopology(null, drpc);
			cluster.submitTopology("tfidf", conf, topology.build());
			File file_tfidf = new File("/Users/walter/Documents/jianguo/workspace/lambda/files/tfidf.txt");
			File file_source = new File("/Users/walter/Documents/jianguo/workspace/lambda/files/doc2.txt");
//			for(int i=0; i<100; i++) {
				System.out.println("About to query!");
				List<String> list = FileUtils.readLines(file_source, "utf8");
				for(String line : list){
					long start = System.currentTimeMillis();
					 FileUtils.writeStringToFile(file_tfidf, drpc.execute("tfidfQuery", line)+", elapse time:"+(System.currentTimeMillis()-start)+"\n",true);
					 
		                Utils.sleep(1000);
				}
               
//            }
//			File file_df = new File("/Users/walter/Documents/jianguo/workspace/lambda/files/df.txt");
//			for(int i=0; i<1; i++) {
//				System.out.println("About to query!");
//                FileUtils.writeStringToFile(file_df, drpc.execute("dfQuery", "雨水")+"\n",false);
////                Utils.sleep(1000);
//            }
			
//			File file_d = new File("/Users/walter/Documents/jianguo/workspace/lambda/files/d.txt");
//			for(int i=0; i<1; i++) {
//				System.out.println("About to query!");
//                FileUtils.writeStringToFile(file_d, drpc.execute("dQuery", "article")+"\n",false);
////                Utils.sleep(1000);
//            }
		} else {
			conf.setNumWorkers(6);
		}
	}

}

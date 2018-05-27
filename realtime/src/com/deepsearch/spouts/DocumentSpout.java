package com.deepsearch.spouts;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;


public class DocumentSpout implements IBatchSpout{

	private static final long serialVersionUID = 797463223717184806L;
	private SpoutOutputCollector collector;
	
	private String mqUrl;
	private String exchange;
	private String queueName;
	private String routeKey;
	
	Fields fields;
	List<Object>[] outputs;
	int maxBatchSize;
	HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();

	public DocumentSpout(Fields fields, int maxBatchSize) {
	    this.fields = fields;
//	    this.outputs = outputs;
	    this.maxBatchSize = maxBatchSize;
	}

	int index = 0;
//	boolean cycle = false;

	public void setCycle(boolean cycle) {
//	    this.cycle = cycle;
	}

	@Override
	public void open(Map conf, TopologyContext context) {
	    index = 0;
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
	    List<List<Object>> batch = this.batches.get(batchId);
	    if(batch == null){
	        batch = new ArrayList<List<Object>>();
	        if(index>=outputs.length) {
	            index = 0;
	        }
	        for(int i=0; index < outputs.length && i < maxBatchSize; index++, i++) {
	            batch.add(outputs[index]);
	        }
	        this.batches.put(batchId, batch);
	    }
	    for(List<Object> list : batch){
	        collector.emit(list);
	    }
	}

	@Override
	public void ack(long batchId) {
	    this.batches.remove(batchId);
	}

	@Override
	public void close() {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
	    Config conf = new Config();
	    conf.setMaxTaskParallelism(1);
	    return conf;
	}

	@Override
	public Fields getOutputFields() {
	    return fields;
	}



    /*@Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TfidfTopologyFields.DOCUMENT, 
        		TfidfTopologyFields.DOCUMENT_ID,
        		TfidfTopologyFields.SOURCE));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
       
        this.collector = spoutOutputCollector;
        mqUrl = Conf.MQ_URL;
        exchange = Conf.MQ_EXCHANGE;
        queueName = Conf.MQ_QUEUENAME;
        routeKey = Conf.MQ_ROUTEKEY;
    }


    @Override
    public void nextTuple() {
        
    		List<String> lines = MQUtils.readArticleMQ(mqUrl, exchange, queueName, routeKey, 1000);
        if(lines==null || lines.size()==0) {
            try { Thread.sleep(50); } catch (InterruptedException e) {}
        } else {
        		for(String line : lines){
        			String[] array = line.split("\t");
        			collector.emit(new Values(array[1],array[0],array[2]));
        		}
            
        }
    }*/
    
}

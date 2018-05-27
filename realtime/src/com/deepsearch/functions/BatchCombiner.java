package com.deepsearch.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class BatchCombiner extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(BatchCombiner.class);
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		try {
			double d_rt =  getValue(tuple, "d_rt");
			double df_rt = getValue(tuple, "df_rt");
			double tf_rt = getValue(tuple, "tf_rt");

			double d_batch = getValue(tuple, "d_batch");
			double df_batch = getValue(tuple, "df_batch");
			double tf_batch = getValue(tuple, "tf_batch");

			LOG.debug("Combining! d_rt=" + d_rt + "df_rt=" + df_rt + "tf_rt="
					+ tf_rt + "d_batch=" + d_batch + "df_batch=" + df_batch
					+ "tf_batch=" + tf_batch);

			collector.emit(new Values(tf_rt + tf_batch, d_rt + d_batch, df_rt
					+ df_batch));
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	private double getValue(TridentTuple tuple, String fieldName){
		if(tuple.getLongByField(fieldName) == null){
			return 0;
		}else{
			return (double)tuple.getLongByField(fieldName);
		}
	}
	

}

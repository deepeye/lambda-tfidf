package com.deepsearch.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class PrintFunction extends BaseFunction {

	Logger LOG = LoggerFactory.getLogger(PrintFunction.class);
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		int size = tuple.size();
		String info = "current document:";
		for(int i=0;i<size;i++){
			info += tuple.getString(i)+";";
		}
		LOG.debug(info);
	}

}

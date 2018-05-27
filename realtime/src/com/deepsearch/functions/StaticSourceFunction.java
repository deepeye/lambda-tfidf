package com.deepsearch.functions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class StaticSourceFunction extends BaseFunction{

	static Logger LOG = LoggerFactory.getLogger(StaticSourceFunction.class);
	private static final long serialVersionUID = 1L;

	private String source;
	public StaticSourceFunction(String source){
		this.source = source;
	}
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	LOG.debug("Emitting static value");
        collector.emit(new Values(source));
    }
}

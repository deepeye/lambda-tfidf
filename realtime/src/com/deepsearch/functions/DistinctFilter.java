package com.deepsearch.functions;

import java.util.HashSet;
import java.util.Set;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DistinctFilter extends BaseFunction{
	
	private static final long serialVersionUID = -3867182073835800908L;

	private Set<String> set = new HashSet<String>();

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String docId = tuple.getStringByField("documentId");
		String term = tuple.getStringByField("term");
		String key = String.join(docId, term);
		if(!set.contains(key)){
			set.add(key);
			collector.emit(new Values(docId, term));
		}
	}
	
	
//	@Override
//	public boolean isKeep(TridentTuple tuple) {
//		String docId = tuple.getStringByField("documentId");
//		String term = tuple.getStringByField("term");
//		String key = String.join(docId, term);
//		if(set.contains(key)){
//			return false;
//		}else{
//			set.add(key);
//			return true;
//		}
//	}

}

package com.deepsearch.functions;

import java.util.List;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.deepsearch.TfidfTopologyFields;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DocumentTokenizer extends BaseFunction {
	
	Logger LOG = LoggerFactory.getLogger(DocumentTokenizer.class); 

	private static final long serialVersionUID = 1L;
	

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String documentContents = tuple.getStringByField(TfidfTopologyFields.DOCUMENT);
		Result result = ToAnalysis.parse(documentContents);
		List<Term> list = result.getTerms();
		for(Term term : list){
			collector.emit(new Values(term.getRealName()));
		}
	}

}

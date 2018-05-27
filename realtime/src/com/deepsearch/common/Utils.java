package com.deepsearch.common;

import java.util.List;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;

public class Utils {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String documentContents = "doc1\t“十三五”深化医药卫生体制改革规划》昨出炉 支持符合条件的企业直接融资";
		Result result = ToAnalysis.parse(documentContents.split("\t")[1]);
		List<Term> list = result.getTerms();
		for(Term term : list){
			System.out.println(term.getRealName());
		}
		
		/*double d = 154;
		double df = 29;
		double tf = 29;
		double tfidf = tf * Math.log(d / (1 + df));
		System.out.println(tfidf);*/
	}
}

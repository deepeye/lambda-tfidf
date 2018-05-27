package com.deepsearch.trident.state;

import java.util.List;
import java.util.Map;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;


public class HBaseBucketState<T> extends HBaseMapState<T> {

	public HBaseBucketState(
			Options<T> options,
			Map map) {
		super(options, map);
	}
	
	public static StateFactory opaque() {
        return opaque(new Options<OpaqueValue>());
    }

    public static StateFactory opaque(Options<OpaqueValue> opts) {
        return new Factory(StateType.OPAQUE, opts);
    }

    public static StateFactory transactional() {
        return transactional(new Options<TransactionalValue>());
    }

    public static StateFactory transactional( Options<TransactionalValue> opts) {
        return new Factory(StateType.TRANSACTIONAL, opts);
    }

    public static StateFactory nonTransactional() {
        return nonTransactional(new Options<Object>());
    }

    public static StateFactory nonTransactional(Options<Object> opts) {
        return new Factory(StateType.NON_TRANSACTIONAL,  opts);
    }

	protected static class Factory extends HBaseMapState.Factory {

        public Factory(StateType stateType, Options options) {
        		super(stateType, options);
        }

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public State makeState(Map map, IMetricsContext metrics,
				int partitionIndex, int numPartitions) {
			return super.makeState(map,metrics,partitionIndex,numPartitions);
		}
    }
	
	@Override
    public List<T> multiGet(List<List<Object>> keys) {
        return super.multiGet(keys);
    }
	
    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
    		super.multiPut(keys, values);
    }

}

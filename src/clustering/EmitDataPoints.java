package clustering;

import backtype.storm.spout.SpoutOutputCollector;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import java.util.Random;

import clustering.database.ClusterDatabase;

public class EmitDataPoints extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Integer count=0;
    Random _rand=new Random();

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    
    }

    public void nextTuple() {
        Utils.sleep(100);
    	String ser_matrices[];
        int[][][] matrices = {{{1,2,3},{4,5,6}},{{3,4,5},{6,7,8}}};
        ser_matrices=ClusterDatabase.get_serialized_two_d_matrices();
        EmitMatrixWithClosestCluster.emit_matrix_with_closes_cluster_center(_collector, ser_matrices,matrices[_rand.nextInt(matrices.length)]);
    }
    
    

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("index","matrix"));
    }
    
}
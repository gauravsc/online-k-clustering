package clustering;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import clustering.database.SerializerAndDeserializer;
import clustering.database.ClusterDatabase;
public class ClusteringTopology {
	
	public static void intializeDB(){
		
		int matrices[][][]={{{1,2,3},{1,5,6}},
				{{1,3,4},{5,7,8}}};
		String ser_matrix="";
		for(int i=0;i<matrices.length;i++){
			ser_matrix=SerializerAndDeserializer.serialize(matrices[i]);
			ClusterDatabase.set_serialized_two_d_matix(i+1, ser_matrix);
		}
	}
	public static void main(String args[]) throws Exception{
		intializeDB();
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("emit_matrices", new EmitDataPoints(), 1);        
        builder.setBolt("create_clusters", new ClusterDataPoints(), 1)
                             .fieldsGrouping("emit_matrices", new Fields("index"));
                
                
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
	}

}

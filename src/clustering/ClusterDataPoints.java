package clustering;
import clustering.database.SerializerAndDeserializer;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import clustering.database.ClusterDatabase;

public class ClusterDataPoints extends BaseRichBolt{

	
	OutputCollector _collector;
	HashMap<Integer,int[][]>matrices_hashmap=new HashMap<Integer,int[][]>();
    Map<String, Integer> counts = new HashMap<String, Integer>();
    int [] n=new int[30];
    
    public void getInitialMatricesFromDB(){
    	String [] ser_matrices_from_db=ClusterDatabase.get_serialized_two_d_matrices();
    	for (int i=0;i<ser_matrices_from_db.length;i++){
    		try{
    		this.matrices_hashmap.put(i+1,SerializerAndDeserializer.deserialize(ser_matrices_from_db[i]));
    		}catch(Exception e){
    			e.printStackTrace();
    		}
    	}
    
    	
    }
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    	
        _collector = collector;
        getInitialMatricesFromDB();
    }
    
    
    public void updateDB(int index, int[][]matrix_to_update){
    	String matrix_serial=SerializerAndDeserializer.serialize(matrix_to_update);
    	ClusterDatabase.set_serialized_two_d_matix(index, matrix_serial);
    	
    }
    
    public void execute(Tuple tuple) {
    	int index= (int)tuple.getInteger(0);
    	int result_matrix[][]=new int[2][];
    	int [][]matrix=(int[][])tuple.getValue(1);
    	int[][]centroid_matrix=matrices_hashmap.get(index);
    	//System.out.println("in bolts"+centroid_matrix);
    	if(centroid_matrix!=null&&matrix!=null){
    		n[index]+=1;
    		result_matrix=MatrixOperations.add_matrices(matrix,centroid_matrix,n[index]);
    		matrices_hashmap.put(index,result_matrix);
    		updateDB(index,result_matrix);
    	}/*else{
    		result_matrix=matrix;
    		matrices_hashmap.put(index,matrix);
    		updateDB(index,matrix);
    	}*/
        _collector.emit(new Values(index,result_matrix));
        _collector.ack(tuple);
    }

    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("index","matrix"));
    }

	
}

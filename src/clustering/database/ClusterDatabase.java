package clustering.database;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;


public class ClusterDatabase {
    
    private static StringSerializer stringSerializer = StringSerializer.get();
   
    public static String[] get_serialized_two_d_matrices(){
    	Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        Keyspace keyspaceOperator = HFactory.createKeyspace("twissandra", cluster);
        Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
    	String [] serialized_matrices= new String [2],clusters={"cluster-1","cluster-2"};
    	try{
	    	ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
	    	for (int i=0;i<clusters.length;i++){
	    		    columnQuery.setColumnFamily("user").setKey("cluster").setName(clusters[i]);
	    	    	QueryResult<HColumn<String, String>> result = columnQuery.execute();
	    	    	serialized_matrices[i]=result.get().getValue();
	    	}   
    	
    	}catch(HectorException e){
    		e.printStackTrace();
    	}
    	return serialized_matrices;
    	
    }
    public static void set_serialized_two_d_matix(int index_of_matrix, String serialized_matrix){
    	Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        Keyspace keyspaceOperator = HFactory.createKeyspace("twissandra", cluster);
        Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
        try{
    	mutator.insert("cluster", "user", HFactory.createStringColumn("cluster-"+index_of_matrix,serialized_matrix));
        }catch(HectorException e){
        	e.printStackTrace();
        }
        
        }
        
    
   
    
}
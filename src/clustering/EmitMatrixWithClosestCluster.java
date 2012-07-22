package clustering;
import backtype.storm.spout.SpoutOutputCollector;
import clustering.database.SerializerAndDeserializer;
import java.io.IOException;
import backtype.storm.tuple.Values;


public class EmitMatrixWithClosestCluster {
	
    public static int difference_between_matrices(int [][] matrix1, int [][]matrix2){
    	int diff=0;
    	for(int i=0;i< matrix1.length;i++){
    		for(int j=0;j<matrix1[i].length;j++){
    			diff+=Math.abs(matrix1[i][j]-matrix2[i][j]);
    		}
    	}
    	return diff;
    	
    }
	public static void emit_matrix_with_closes_cluster_center(SpoutOutputCollector _collector,String serialized_matrices[],int [][]matrix ){
		int min=1000000, diff,index=0;
		int [][] deser_matrix;
		int [][] return_matrix=null;
		for(int i=0;i<serialized_matrices.length;i++){
			try{
				deser_matrix=SerializerAndDeserializer.deserialize(serialized_matrices[i]);
				diff=difference_between_matrices(deser_matrix,matrix);
			if(diff<min){
			min=diff;
			index=i+1;
			return_matrix=deser_matrix;
			}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
		_collector.emit(new Values(index,matrix));
		
	}

}

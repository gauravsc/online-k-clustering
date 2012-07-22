package clustering;

public class MatrixOperations {
	
	public static int[][] add_matrices(int[][]matrix1,int[][]matrix2,int value_of_n){
		
		int [][]matrix3=new int[matrix1.length][matrix1[0].length];
		for(int i=0;i<matrix1.length;i++){
			for(int j=0;j<matrix1[i].length;j++){
				matrix3[i][j]=(value_of_n*matrix2[i][j]+matrix1[i][j])/2;
				
			}
		}
		return matrix3;
	}

}

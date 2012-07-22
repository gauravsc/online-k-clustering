package clustering.database;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;

public class SerializerAndDeserializer {
	private static final char NEXT_ITEM = ' ';

	public static String serialize(int[][] array) {
	    StringBuilder s = new StringBuilder();
	    s.append(array.length).append(NEXT_ITEM);

	    for(int[] row : array) {
	        s.append(row.length).append(NEXT_ITEM);

	        for(int item : row) {
	            s.append(String.valueOf(item)).append(NEXT_ITEM);
	        }
	    }

	    return s.toString();
	}

	public static int[][] deserialize(String str) throws IOException {
	    StreamTokenizer tok = new StreamTokenizer(new StringReader(str));
	    tok.resetSyntax();
	    tok.wordChars('0', '9');
	    tok.whitespaceChars(NEXT_ITEM, NEXT_ITEM);
	    tok.parseNumbers();

	    tok.nextToken();

	    int     rows = (int) tok.nval;
	    int[][] out  = new int[rows][];

	    for(int i = 0; i < rows; i++) {
	        tok.nextToken();

	        int   length = (int) tok.nval;
	        int[] row    = new int[length];
	        out[i]       = row;

	        for(int j = 0; j < length; j++) {
	            tok.nextToken();
	            row[j] = (int) tok.nval;
	        }
	    }

	    return out;
	}

}

package com.praveenak.tsa;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Stem extends EvalFunc<String> {
	Stemmer s = new Stemmer();
@Override
	public String exec( Tuple input ) throws IOException {
		try {
	        if (input==null)
	            return null;
	        if (input.size()==0)
	            return null;
	        Object o = input.get(0);
	        if (o==null)
	            return null;
	        if (!(o instanceof String)) {
	        	int errCode = 2114;
	        	String msg = "Expected input to be chararray, but" +
	            " got " + o.getClass().getName();
	            throw new ExecException(msg, errCode, PigException.BUG);
	        }
	    	int i = 0;
			String adjective = (String)input.get(0);
			char[] charray = adjective.toCharArray();
			char[] w = new char[501];
			char ch = charray[i];
			if (Character.isLetter((char) ch)){
	                 int j = 0;
	                 while(true){
	                	 ch = Character.toLowerCase((char)ch);
	                	 w[j] = (char) ch;
	                	 if (j < 500) j++;
	                	 if(i < charray.length - 1){
	                		 	ch = charray[++i];
	                	 		if (!Character.isLetter((char) ch))
	                	 		{
	                	 			/* to test add(char ch) */
	                	 			for (int c = 0; c < j; c++) s.add(w[c]);
	                	 			/* or, to test add(char[] w, int j) */
	                	 			/* s.add(w, j); */
	                	 			s.stem();
	                	 			{  
	                	 				String u;
	                	 				/* and now, to test toString() : */
	                	 				u = s.toString();
	                	 				/* to test getResultBuffer(), getResultLength() : */
	                	 				/* u = new String(s.getResultBuffer(), 0, s.getResultLength()); */
	                	 				//System.out.print(u);
	                	 				return u;
	                	 			}
	                	 		}
	                	 }
	                	 else{
	                		 	/* to test add(char ch) */
	         	 				for (int c = 0; c < j; c++) s.add(w[c]);
	         	 				/* or, to test add(char[] w, int j) */
	         	 				/* s.add(w, j); */
	         	 				s.stem();
	         	 				{  
	         	 					String u;
	         	 					/* and now, to test toString() : */
	         	 					u = s.toString();
	         	 					/* to test getResultBuffer(), getResultLength() : */
	         	 					/* u = new String(s.getResultBuffer(), 0, s.getResultLength()); */
	         	 					//System.out.print(u);
	         	 					return u;
	         	 				}
	                	 }
	                 }
	        }
	       	if ((int)ch < 0)
	          		System.out.print((char)ch);
			return null;
		}
		catch (ExecException ee) {
	        throw ee;
	    }
	}

	public static void main( String args[] ) throws IOException
	{
		Stem stem = new Stem();
		Tuple tuple = TupleFactory.getInstance().newTuple( 1 );
		tuple.set( 0, new String( "incompetent" ) );
		System.out.println( stem.exec( tuple ) );
	}
}
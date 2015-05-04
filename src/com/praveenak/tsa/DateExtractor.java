package com.praveenak.tsa;


import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class DateExtractor extends EvalFunc<String> {
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
		String date[] = ((String)input.get(0)).toLowerCase().split( " " );
		return date[2];
	}
	catch (ExecException ee) {
        throw ee;
    }}

	public static void main( String args[] ) throws IOException
	{
		DateExtractor date = new DateExtractor();
		Tuple tuple = TupleFactory.getInstance().newTuple( 1 );
		tuple.set( 0, new String( "tue tue 02 tue" ) );
		System.out.println( date.exec( tuple ) );
	}
}

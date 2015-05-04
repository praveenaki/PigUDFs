package com.praveenak.tsa;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class Id extends EvalFunc<Integer> {
	int i=0;
@Override
	public Integer exec( Tuple input ) throws IOException {
		if( input == null || input.size() == 0 )
			return null;
		else	
		{
			i++;
				return i;
		}
	}
	public static void main( String args[] ) throws IOException{
		Id game = new Id();
		Tuple tuple = TupleFactory.getInstance().newTuple( 1 );
		tuple.set( 0, new String( "GET /info/games/Call_of_Duty.html HTTP/1.1" ) );
		System.out.println( game.exec( tuple ) );
		}
}

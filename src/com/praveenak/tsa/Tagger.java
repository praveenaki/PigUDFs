package com.praveenak.tsa;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.FuncSpec;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class Tagger extends EvalFunc<DataBag>{
	TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();
    MaxentTagger tagger = null;
    public DataBag exec(Tuple input) throws IOException {
    	
    	try {
    		 if (input==null)
                 return null;
             if (input.size()==0)
                 return null;
             Object o = input.get(0);
             if (o==null)
                 return null;
            DataBag output = mBagFactory.newDefaultBag();
            if (!(o instanceof String)) {
            	int errCode = 2114;
            	String msg = "Expected input to be chararray, but" +
                " got " + o.getClass().getName();
                throw new ExecException(msg, errCode, PigException.BUG);
            }
            if (tagger == null)
				tagger = new MaxentTagger("taggers/left3words-wsj-0-18.tagger");
            String tagged = tagger.tagString((String)o);
            String[] tags;
            tags = tagged.split(" ");
            String adjTweet = new String();
            for(int i = 0; i < tags.length; i++) {
            	String suffix = tags[i].substring(tags[i].indexOf("/")+1);
            	if ( suffix.equals("JJ")){
            		String prefix = tags[i].substring(0,tags[i].indexOf("/") );
            		adjTweet += (prefix+" ");
            	}
            }
            adjTweet = adjTweet.trim();
            StringTokenizer tok = new StringTokenizer(adjTweet, " \",()*", false);
            while (tok.hasMoreTokens()) output.add(mTupleFactory.newTuple(tok.nextToken()));
            return output;
    	}
            catch (ExecException ee) {
                throw ee;
            } catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		return null;
    }
    
    @SuppressWarnings("deprecation")
    @Override
    public Schema outputSchema(Schema input) {
        
        try {
            Schema.FieldSchema tokenFs = new Schema.FieldSchema("token", 
                    DataType.CHARARRAY); 
            Schema tupleSchema = new Schema(tokenFs);

            Schema.FieldSchema tupleFs;
            tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
                    DataType.TUPLE);

            Schema bagSchema = new Schema(tupleFs);
            bagSchema.setTwoLevelAccessRequired(true);
            Schema.FieldSchema bagFs = new Schema.FieldSchema(
                        "bag_of_tokenTuples",bagSchema, DataType.BAG);
            
            return new Schema(bagFs); 
            
            
            
        } catch (FrontendException e) {
            // throwing RTE because
            //above schema creation is not expected to throw an exception
            // and also because superclass does not throw exception
            throw new RuntimeException("Unable to compute TOKENIZE schema.");
        }   
    }

    public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
        List<FuncSpec> funcList = new ArrayList<FuncSpec>();
        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
        funcList.add(new FuncSpec(this.getClass().getName(), s));
        return funcList;
    }
    
    public static void main( String args[] ) throws IOException
	{
    	Tagger game = new Tagger();
		Tuple tuple = TupleFactory.getInstance().newTuple( 1 );
		tuple.set( 0, new String( "This is good very good not very bad" ) );
		System.out.println( game.exec( tuple ) );
	}

}

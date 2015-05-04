package com.praveenak.tsa;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.*;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

public class Polarity extends EvalFunc<String> {
	
	public HashMap<String, Integer> dict = null;
	public HashMap<String, String> freq_list = null;
	public double tpp = 1.0;
	public double tpn = 1.0;
	public double denum = 1.0;
	public double pp = 0.0;
	public double np = 0.0;
	MaxentTagger tagger = null;
	
	@Override
	public String exec( Tuple input ) throws IOException{
		tpp = 1.0;
		tpn = 1.0;
		denum = 1.0;
		pp = 0.0;
		np = 0.0;
		if( input == null || input.size() == 0 )
			return null;
		else
		{
			try{
			
				String tweet = (String)input.get(0);
				if(tweet == null){
					return null;
				}	
			
				if (!(tweet instanceof String)) {
					int errCode = 2114;
					String msg = "Expected input to be chararray, but" + " got " + tweet.getClass().getName();
	                throw new ExecException(msg, errCode, PigException.BUG);
	            }
				if(tagger == null)
					tagger = new MaxentTagger("taggers/left3words-wsj-0-18.tagger");
				if(freq_list == null){
					String line = null;
					String [] line_parts = null;
					freq_list = new HashMap<String, String>();
					FileInputStream fis = new FileInputStream("/home/notroot/lab/programs/SENTIMENT_ANALYSIS/adjectives_frequencies.txt");
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					while((line = br.readLine()) != null){
						line_parts = line.split(" ");
						freq_list.put(line_parts[0], line_parts[1] + " " + line_parts[2]);
					}
					System.out.println(freq_list.entrySet());
				}
			
			
			String tagged = tagger.tagString(tweet);	
			try {
				extract_adjectives(tagged);
			} catch (Exception e) {
				e.printStackTrace();
			}
			//System.out.println(dict.entrySet());
			Set set = dict.keySet();
			Iterator it = set.iterator();
			int countadj = 0;
			while(it.hasNext()){
				String adjective = (String) it.next();
				countadj += dict.get(adjective);
				
				try {
					pp = polar(adjective, dict.get(adjective), "positive");
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					np = polar(adjective, dict.get(adjective), "negative");
				} catch (Exception e) {
					e.printStackTrace();
				}
				tpp = tpp*pp;
				tpn = tpn*np;
				
				
			}
			int fact = 1;
			for(int i=1; i<=countadj; i++){
				fact = fact * i;
			}
			
			tpp = tpp*fact;
			tpn = tpn*fact;
			denum = ((0.5 * tpp) + (0.5 * tpn));
			tpp = (0.5 * tpp)/denum;
			tpn = (0.5 * tpn)/denum;
		
			System.out.println("Positive " + tpp);
			System.out.println("Negative " + tpn);
	
			if(tpp > tpn){
				return ("positive");
			}
			else if(tpp < tpn){
				return("negative");
			}
			else{
				return("neutral");
			}			
		}
			catch (ExecException ee) {
	            throw ee;
	        } catch (ClassNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		return null;
	}
			
	public void extract_adjectives(String tagged) throws Exception{
		System.out.println("tagged="+tagged);
		String[] tags;
		tags = tagged.split(" ");
		String stemmed_prefix = null;
		dict= new HashMap<String, Integer>();
		for(int i = 0; i < tags.length; i++) {
			String suffix = tags[i].substring(tags[i].indexOf("/")+1);
			//System.out.println(suffix);
			if ( suffix.equals("JJ")){
				String prefix = tags[i].substring(0,tags[i].indexOf("/") );
				stemmed_prefix = start_stem(prefix);
				if(dict.containsKey(stemmed_prefix)){
					dict.put(stemmed_prefix, dict.get(stemmed_prefix)+1);
				}
				else{
					dict.put(stemmed_prefix, 1);
				}
			}
		}
		System.out.println("This is the dictionary" + dict.entrySet());
	}
	
	public double polar(String adjective, int frequency, String sign) throws Exception{
		if(sign.equals("positive")){
			int l = 2908;
			int	lp = 13376;
			int	ln = 12546;
			int count = 1;
			int fact = 1;
			int i=1;
			double polarity = 0.0;
			double polarity_exponent = 1.0;
			String line = null;
			String [] line_parts = null;
			line = freq_list.get(adjective);
			line_parts = line.split(" ");
			count += Integer.parseInt(line_parts[0]);
			polarity = (count*1.0)/(l+lp+ln);
			for(i=1; i<=frequency; i++){
				polarity_exponent = polarity_exponent*polarity;
			}
			polarity = polarity_exponent;
			for(i=1; i<=frequency; i++){
				fact = fact * i;
			}
			polarity = (polarity*1.0)/fact;
			return polarity;
		}
		if(sign.equals("negative")){
			int l = 2908;
			int	lp = 13376;
			int	ln = 12546;
			int count = 1;
			int fact = 1;
			int i=1;
			double polarity = 0.0;
			double polarity_exponent = 1.0;
			String line = null;
			String [] line_parts = null;
			line = freq_list.get(adjective);
			line_parts = line.split(" ");
			count += Integer.parseInt(line_parts[1]);
			polarity = (count*1.0)/(l+lp+ln);
			System.out.println(polarity);
			for(i=1; i<=frequency; i++){
				polarity_exponent = polarity_exponent*polarity;
			}
			polarity = polarity_exponent;
			for(i=1; i<=frequency; i++){
				fact = fact * i;
			}
			polarity = (polarity*1.0)/fact;
			return polarity;
		}
		return 0.0;
	}
	
		public String start_stem(String adjective){
	      	int i = 0;
			Stemmer s = new Stemmer();
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
		

	public static void main(String[] args) throws Exception{
		Polarity p = new Polarity();
		p.start_stem("Nikhil's");
		Tuple tuple = TupleFactory.getInstance().newTuple( 1 );
		tuple.set( 0, new String( "He is a good GOOd GREAT GreaT good good good goodness amazingly humble honest horriblly stupid and an idiotic man." ) );
		System.out.println(p.exec( tuple ));
	}
}

package com.df.usecase;

/*
* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.transforms.SerializableFunction;


public class Join {
	
	
	static class mapKeyValues extends DoFn<String, KV<String,String>> 
	{
		
	    @ProcessElement
	    public void processElement(ProcessContext c) 
	    {
	    	
	    		String[] key_values = c.element().split(",");
	    		//System.out.println(counter);
	    		String str="";
	    		for(int i=1;i<key_values.length;i++)
	    		{
	    			str=str+key_values[i]+",";
	    		}
	    		c.output(KV.of(key_values[0], str));
	    		//c.output(KV.of(team_goals[2], Long.parseLong(team_goals[4])));
	    }
	}
	
	
	
	
	
	public static class KeyValues extends PTransform<PCollection<String>,
    PCollection<KV<String, Iterable<String>>>> {
		public PCollection<KV<String, Iterable<String>>> expand(PCollection<String> lines) {

			PCollection<KV<String, Iterable<String>>> KeyValues = lines.apply(
			ParDo.of(new mapKeyValues()))
    		.apply(GroupByKey.create());
    		return KeyValues;
		}
	}
	
	
	public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
	    @Override
	    public String apply(KV<String, Long> input) {
	      return input.getKey() + ": " + input.getValue();
	    }
	  }
	
	

  public static void main(String[] args) {
	  		
	  		PipelineOptions options = PipelineOptionsFactory.create();
		    
		     Pipeline p = Pipeline.create(options);
		    PCollection<String> gCollection=p.apply(TextIO.read().from("gs://abhishekdataflowbucket/Total_Goals"));
		    PCollection<String> pCollection=p.apply(TextIO.read().from("gs://abhishekdataflowbucket/Team_Info_No*"));
		    PCollection<KV<String,Iterable<String>>> gCollectionKV=gCollection.apply(new KeyValues());
		    PCollection<KV<String,Iterable<String>>> pCollectionKV=pCollection.apply(new KeyValues());
		    
		    pCollection.apply(new KeyValues());
		     .apply(MapElements.via(new FormatAsTextFn()));
		    aCollection.apply(TextIO.write().to("gs://abhishekdataflowbucket/Total_Goals").withoutSharding()) 	;
		    p.run().waitUntilFinish();
		  
		   
		    // Create tuple tags for the value types in each collection.
		    final TupleTag<Iterable<String>> tag1 = new TupleTag<Iterable<String>>();
		    final TupleTag<Iterable<String>> tag2 = new TupleTag<Iterable<String>>();

		    // Merge collection values into a CoGbkResult collection.
		    PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
		      KeyedPCollectionTuple.of(tag1, gCollection)
		                           .and(tag2, pCollection)
		                           .apply(CoGroupByKey.<String>create());
	  		 
		  }
    
  }
 



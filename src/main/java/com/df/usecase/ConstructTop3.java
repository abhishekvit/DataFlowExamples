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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.SerializableFunction;



//refer design pattern reducer
public class ConstructTop3 {
	
	static class mapTeamToGoals extends DoFn<String, KV<String,Long>> 
	{
		
	    @ProcessElement
	    public void processElement(ProcessContext c) 
	    {
	    	String[] team_goals = c.element().split(": ");
	    	c.output(KV.of(team_goals[0],Long.parseLong(team_goals[1])));
	    }
	  }
	
	
	static class sumPoints implements SerializableFunction<Iterable<Long>, Long> {
		@Override
		public Long apply(Iterable<Long> values) {
			// TODO Auto-generated method stub
			long sum = 0;
            for (Long value : values) {
              sum += value;
            }
            return sum;

		}
}

	
	public static class CountWords extends PTransform<PCollection<String>,
    PCollection<KV<String, Long>>> {
		@Override
		public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

			// Convert lines of text into individual words.
			PCollection<KV<String, Long>> sumPoints = lines.apply(
			ParDo.of(new mapTeamToGoals()));
    		
    		
			return sumPoints;
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
		    PCollection<String> aCollection=p.apply(TextIO.read().from("gs://abhishekdataflowbucket/Total_Points"))
		     .apply(new CountWords())
		     .apply(MapElements.via(new FormatAsTextFn()));
		    aCollection.apply(TextIO.write().to("gs://abhishekdataflowbucket/Top3").withoutSharding()) 	;
		    p.run().waitUntilFinish();
		  }
    
  }
 




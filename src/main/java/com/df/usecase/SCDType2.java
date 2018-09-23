package com.df.usecase;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import com.df.usecase.ConstructPoints.FormatAsTextFn;
import com.google.common.collect.Iterables;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class SCDType2 {
	
	
	
	static String flag;
	static class mapKeyValue extends DoFn<String, KV<String, String>> {

		@ProcessElement
		public void processElement(ProcessContext c) 
		{
			System.out.println("mkv"+flag);
			
			String[] words = c.element().split(",");
				
				//System.out.println(flag);
				String values="";
				for(int i=2;i<words.length;i++)
				{
					values=values+","+words[i];
				}
				//System.out.println(values);
				c.output(KV.of(words[0]+","+words[1],values.substring(1)));
				
		}
	}
	
	static class mapKeyValue1 extends DoFn<KV<String,Iterable<String>>, KV<String, String>> {

		@ProcessElement
		public void processElement(ProcessContext c) 
		{
			
			
			Iterable<String> values =  c.element().getValue();
			
			int l=Iterables.size(values);
			
			//System.out.println(c.element().getKey()+","+l);
			
			for(String value:values)
			{
				//System.out.println(value);
				String data[]=value.split(",");
				int len=data.length;
				
				if(l==1)
				{
					
					String str = String.join(",", data);
						c.output(KV.of(c.element().getKey(),str));
					
				}
				if(l==2)
				{
					if(len==3)
					{
						DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
						Date date = new Date();
						Calendar cal = Calendar.getInstance();
						cal.setTime(date);
						cal.add(Calendar.DATE, -1);
						String d=dateFormat.format(cal.getTime()); 
						data[2]=d;
						String str = String.join(",", data);
						c.output(KV.of(c.element().getKey(),str));
					}
					else
					{
						DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
						Date date = new Date();
						String d=(dateFormat.format(date)); //2016/11/16 12:08:43

						c.output(KV.of(c.element().getKey(),value+","+d+","+"9999-12-31"));
					}
				}
				else
				{
					
				}
			}
			
				
		}
	}

	

	
	public static class FormatAsTextFn extends SimpleFunction<KV<String, String>, String> {
		@Override
		public String apply(KV<String, String> input) {
			return input.getKey() +"," + input.getValue();
		}
	}
	
	public static void main(String[] args) {

		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline p = Pipeline.create(options);
		//SCDType2 obj=new SCDType2();
		flag="a";
		//System.out.println("A"+flag);
		PCollection<KV<String,String>> D1Collection = p.apply(TextIO.read().from("gs://abhishekdataflowbucket/Input1")).apply(ParDo.of(new mapKeyValue()));
		//System.out.println("end");
		PCollection<KV<String,String>> D2Collection=p.apply(TextIO.read().from("gs://abhishekdataflowbucket/Input2")).apply(ParDo.of(new mapKeyValue()));
		
		PCollectionList<KV<String,String>> pcs = PCollectionList.of(D1Collection).and(D2Collection);
		flag="b";
		//System.out.println("B"+flag);
		PCollection<String> merged = pcs.apply(Flatten.<KV<String,String>>pCollections()).apply(GroupByKey.create()).apply(ParDo.of(new mapKeyValue1())).apply(MapElements.via(new FormatAsTextFn()));
		//System.out.println("end");
		merged.apply(TextIO.write().to("gs://abhishekdataflowbucket/SCD_Out").withoutSharding()) 	;
		p.run().waitUntilFinish();
	}

}

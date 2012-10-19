/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
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

package com.bizosys.hsearch.dictionary;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.TermVector;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class Dictionary {

	public static void main(String[] args) throws Exception {
		StringWriter outputWriter = new StringWriter();
		Dictionary.getInstance().load("https://abinash.s3.amazonaws.com/test.txt", outputWriter);
		System.out.println(outputWriter.toString());
		
		System.out.println( "Found Word :\n" +  Dictionary.getInstance().find("hav") );
	}
	
	private  final static String Empty = ""; 
	private static Dictionary dict = null;
	public static Dictionary getInstance() {
		if ( null != dict) return dict;
		synchronized (Dictionary.class) {
			if ( null != dict) return dict;
			dict = new Dictionary();
		}
		return dict;
	}
	
	Map<String, String> content = new HashMap<String, String>();
	Directory idx = null;
	IndexReader reader = null;
	IndexSearcher searcher = null;

	public Dictionary() {
	}
	
	public void load(String file, Writer outputWriter) throws Exception{
		if ( null != reader ) {
			try {
				reader.close();
				if ( null != searcher ) searcher.close();
			} catch (Exception ex) {
			}
			
		}
		
		this.idx = build(file, outputWriter);
		reader = IndexReader.open(idx);
		searcher = new IndexSearcher(reader);
	}
	
	public String find(String query) throws Exception {
    	
		query = query.toLowerCase();
		if ( content.containsKey(query)) return content.get(query);

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
		String content = search(idx, query, analyzer);
		if ( content == Empty) {
			content = search(idx, query + "~", analyzer);
		}
		return content;
    }

	private String search(Directory idx, String query, Analyzer analyzer) throws ParseException, CorruptIndexException, IOException {
		Query q = new QueryParser(Version.LUCENE_35, "k", analyzer).parse(query);
    	 
    	 int hitsPerPage = 1;
    	 
    	 TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
    	 searcher.search(q, collector);
    	 ScoreDoc[] hits = collector.topDocs().scoreDocs;
    	 int hitsT = ( null != hits ) ? hits.length : 0;
    	 
    	 String desc = Empty;
    	 if ( hitsT > 0 ) {
        	 for(int i=0;i<hits.length;i++) {
        		 int docId = hits[i].doc;
        	     Document d = searcher.doc(docId);
        	     desc = content.get(d.get("k"));
        	 }
    	 }
    	 return desc;
	}
	
	private Directory build(String file, Writer outputWriter) throws Exception {
    	RAMDirectory idx = new RAMDirectory();
    	
    	IndexWriter writer = new IndexWriter(idx,  
    		new IndexWriterConfig(Version.LUCENE_35,new StandardAnalyzer(Version.LUCENE_35)));
    	
    	toLines(file, writer, outputWriter);
    	writer.close();
    	return idx;
	}
    
    private Document createDocument(String k) {
        Document doc = new Document();
        doc.add(new Field("k", true, k, Field.Store.YES, Index.ANALYZED,TermVector.NO) );
        return doc;
    }
    
	public final int toLines(String fileName, IndexWriter writer, Writer outputWriter) throws  Exception {
		
		InputStreamReader stream = null;
		BufferedReader reader = null;

		try {
			URL url = new URL(fileName);
			stream = new InputStreamReader (url.openStream());
			reader = new BufferedReader(stream);
			
			String line = null;
			while((line=reader.readLine())!=null) {
				if (line.length() == 0) continue;
				char first=line.charAt(0);
				switch (first) {
					case ' ' : case '\n' : case '#' :  // skip blank & comment lines
					continue;
				}
				int divideAt = line.indexOf('\t');
				if ( divideAt == -1 ) continue;

				String key = line.substring(0, divideAt).toLowerCase();
				String val = line.substring(divideAt + 1);
				writer.addDocument( createDocument( key) );	
				content.put(key, val);
				outputWriter.append(key).append("</BR>");
			}
			outputWriter.append("----------</BR>Total  Keywords Loaded :  ").append(new Integer(content.size()).toString());
			return content.size();
		} finally {
			if ( null != reader ) reader.close();
			if ( null != stream) stream.close();
		}
	}    
    
}
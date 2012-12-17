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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
		
		StringWriter responseWriter = new StringWriter();
		/**
		Dictionary.getInstance().load("file:///D:/work/hsearch-dictionary/src/test/words.txt", responseWriter);
		*/

		Map<String, String> words = new HashMap<String, String>();
		words.put("john", "fname");
		words.put("abinash", "fname");
		
		Dictionary.getInstance().load(words, responseWriter, false);
		System.out.println(responseWriter.toString());
		System.out.println( "Found Word :" +  Dictionary.getInstance().findTopDocument("joh", true) );
	}
	
	private static Dictionary dict = null;
	public static Dictionary getInstance() {
		if ( null != dict) return dict;
		synchronized (Dictionary.class) {
			if ( null != dict) return dict;
			dict = new Dictionary();
		}
		return dict;
	}
	
	Map<String, String> exactWords = new HashMap<String, String>();
	Directory idx = null;
	IndexReader reader = null;
	IndexSearcher searcher = null;

	public Dictionary() {
	}
	
	public String findTopDocument(String query, boolean isWord) throws Exception {
    	
		query = query.toLowerCase();
		if ( exactWords.containsKey(query)) {
			if ( isWord ) return query;
			else return exactWords.get(query);
		}
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
		Document content = searchTop(idx, query, analyzer);
		if ( content == null) {
			content = searchTop(idx, (query + "~"), analyzer);
			if ( null == content) return null;
		}
			
		if ( isWord) return getWord(content);
		else return getDescription(content);
    }
	
	public List<String> predict(String query) throws Exception {
    	
		query = query.toLowerCase();

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
		List<Document> contents = search(idx, query + "*", analyzer, 10);
		if ( contents == null) return null;
		
		List<String> matchings = new ArrayList<String>();
		for (Document doc : contents) {
			System.out.println(getWord(doc));
			matchings.add( getWord(doc) );
		}
		
		return matchings;
    }	
	
	public boolean containsExact(String query) throws Exception {
    	
		query = query.toLowerCase();
		return ( exactWords.containsKey(query) );
    }	
	
	public String findExact(String query) throws Exception {
    	
		query = query.toLowerCase();
		if ( exactWords.containsKey(query)) return exactWords.get(query);
		return null;
    }	
	
	private Document searchTop(Directory idx, String query, Analyzer analyzer) throws ParseException, CorruptIndexException, IOException {
		Query q = new QueryParser(Version.LUCENE_35, "k", analyzer).parse(query);
    	 
    	 int hitsPerPage = 1;
    	 
    	 TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
    	 searcher.search(q, collector);
    	 ScoreDoc[] hits = collector.topDocs().scoreDocs;
    	 int hitsT = ( null != hits ) ? hits.length : 0;
    	 
    	 if ( hitsT <= 0 ) return null;
    	 return searcher.doc(hits[0].doc);
	}
		

	private List<Document> search(Directory idx, String query, Analyzer analyzer, int hitsPerPage) throws ParseException, CorruptIndexException, IOException {
		Query q = new QueryParser(Version.LUCENE_35, "k", analyzer).parse(query);
    	 
    	 TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
    	 searcher.search(q, collector);
    	 ScoreDoc[] hits = collector.topDocs().scoreDocs;
    	 int hitsT = ( null != hits ) ? hits.length : 0;
    	 
    	 List<Document> foundDocs = new ArrayList<Document>(hitsT);
    	 if ( hitsT > 0 ) {
        	 for(int i=0;i<hits.length;i++) {
        		 int docId = hits[i].doc;
        	     Document d = searcher.doc(docId);
        	     foundDocs.add(d);
        	 }
    	 }
    	 return foundDocs;
	}
	
	private String getDescription(Document d) {
	     return exactWords.get(d.get("k"));
	}
	
	private String getWord(Document d) {
	     return d.get("k");
	}

	private Document createDocument(String k) {
        Document doc = new Document();
        doc.add(new Field("k", true, k, Field.Store.YES, Index.ANALYZED,TermVector.NO) );
        return doc;
    }

	
    public void load(Map<String, String> wordDescription, Writer outputWriter, boolean isExactSave) throws Exception{
		if ( null != reader ) {
			try {
				reader.close();
				if ( null != searcher ) searcher.close();
			} catch (Exception ex) {
			}
		}
		
		this.idx = new RAMDirectory();
    	IndexWriterConfig indexWriterConfig = 
    		new IndexWriterConfig(Version.LUCENE_35,new StandardAnalyzer(Version.LUCENE_35));
		IndexWriter writer = new IndexWriter(this.idx, indexWriterConfig);
    	
		for (String word : wordDescription.keySet()) {
			if (word.length() == 0) continue;
			String desc = wordDescription.get(word);
			writer.addDocument( createDocument(word) );	
			if ( isExactSave ) exactWords.put(word, desc);
			if ( null != outputWriter ) outputWriter.append(word).append("</BR>\n");
		}
		if ( null != outputWriter ) outputWriter.append(
			"----------\n</BR>Total  Keywords Loaded :  ").append(
				new Integer(wordDescription.size()).toString());
		
		writer.close();
		reader = IndexReader.open(this.idx);
		searcher = new IndexSearcher(reader);
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
	    
	private Directory build(String file, Writer outputWriter) throws Exception {
    	RAMDirectory idx = new RAMDirectory();
    	
    	IndexWriter writer = new IndexWriter(idx,  
    		new IndexWriterConfig(Version.LUCENE_35,new StandardAnalyzer(Version.LUCENE_35)));
    	
    	toLines(file, writer, outputWriter);
    	writer.close();
    	return idx;
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
				exactWords.put(key, val);
				if ( null != outputWriter) outputWriter.append(key).append("</BR>\n");
			}
			if ( null != outputWriter) outputWriter.append(
				"----------\n</BR>Total  Keywords Loaded :  ").append(
					new Integer(exactWords.size()).toString());
			return exactWords.size();
		} finally {
			if ( null != reader ) reader.close();
			if ( null != stream) stream.close();
		}
	}    
    
}
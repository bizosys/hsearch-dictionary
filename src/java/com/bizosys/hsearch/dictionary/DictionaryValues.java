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
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

public class DictionaryValues {

	public static void main(String[] args) throws Exception {
		Set<String> words = new HashSet<String>();
		words.add("This is under Women Clothes");
		words.add("This is under Men Clothes");
		
		DictionaryValues.getInstance().load(words, null);
		List<String> output = new ArrayList<String>();
		output = DictionaryValues.getInstance().findDocuments("Baby Clothes", output);
		System.out.println( "Found Word :" +  output.toString() );
	}
	
	private static DictionaryValues dict = null;
	public static DictionaryValues getInstance() {
		if ( null != dict) return dict;
		synchronized (DictionaryValues.class) {
			if ( null != dict) return dict;
			dict = new DictionaryValues();
		}
		return dict;
	}
	
	Directory idx = null;
	IndexReader reader = null;
	IndexSearcher searcher = null;

	public DictionaryValues() {
	}
	
	public List<String> findDocuments(String query, List<String> lines) throws Exception {
		return findDocuments(query, lines, 
			getAnalyzer(), new ArrayList<String>(), new HashSet<Term>() );	
    }
	
	public Analyzer getAnalyzer() {
		return new StandardAnalyzer(Version.LUCENE_35);
	}
	
	public List<String> findDocuments(String query, List<String> lines,
			Analyzer analyzer, List<String> words, Set<Term> terms ) throws Exception {
		
		ScoreDoc[] docs = searchTop(idx, query, analyzer, words, terms);
   	 	int hitsT = ( null != docs ) ? docs.length : 0;
   	 	lines.clear();
   	 	for ( int i=0; i<hitsT; i++) {
   	 		lines.add(searcher.doc(docs[i].doc).get("k"));
   	 	}
		return lines;	
    }

	private ScoreDoc[] searchTop(Directory idx, String query, Analyzer analyzer,
		List<String> words, Set<Term> terms) throws ParseException, CorruptIndexException, IOException {

		fastSplit(words, query, ' ');
		
		QueryParser parser = new QueryParser(Version.LUCENE_35, "k", analyzer);
		PhraseQuery q = new PhraseQuery();
		int location = 0;
		for (String word : words) {
		    Query q1 = parser.parse(word);
		    q1.extractTerms(terms);
			for (Term term : terms) {
				q.add(term, location++);
			}
			terms.clear();
		}
		words.clear();
		q.setSlop(0);
		
		int hitsPerPage = 1;
    	 
		TopScoreDocCollector collector = TopScoreDocCollector.create(hitsPerPage, true);
		searcher.search(q, collector);
		ScoreDoc[] hits = collector.topDocs().scoreDocs;
		return hits;
	}
		

	private Document createDocument(String k) {
        Document doc = new Document();
        doc.add(new Field("k", true, k, Field.Store.YES, Index.ANALYZED,TermVector.NO) );
        return doc;
    }

	
    public int load(Set<String> wordDescription, Writer outputWriter) throws Exception{
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
    	
		int linesAdded = 0;
		for (String line : wordDescription) {
			writer.addDocument( createDocument(line) );	
			linesAdded++;
		}
		if ( null != outputWriter ) outputWriter.append(
			"----------\n</BR>Total  Lines Loaded :  ").append(
				new Integer(linesAdded).toString());
		
		writer.close();
		reader = IndexReader.open(this.idx);
		searcher = new IndexSearcher(reader);
		return linesAdded;
	}
    
	public void loadAndOpen(String file, Writer outputWriter) throws Exception{
		if ( null != reader ) {
			try {
				reader.close();
				if ( null != searcher ) searcher.close();
			} catch (Exception ex) {
			}
			
		}
		
		this.idx = loadFile(file, outputWriter);
		reader = IndexReader.open(idx);
		searcher = new IndexSearcher(reader);
	}
	    
	private Directory loadFile(String file, Writer outputWriter) throws Exception {
    	RAMDirectory idx = new RAMDirectory();
    	
    	IndexWriter writer = new IndexWriter(idx,  
    		new IndexWriterConfig(Version.LUCENE_35,new StandardAnalyzer(Version.LUCENE_35)));
    	
    	loadFileAndIndex(file, writer, outputWriter);
    	writer.close();
    	return idx;
	}
    
	public final int loadFileAndIndex(String fileName, IndexWriter writer, Writer outputWriter) throws  Exception {
		
		InputStreamReader stream = null;
		BufferedReader reader = null;

		try {
			URL url = new URL(fileName);
			stream = new InputStreamReader (url.openStream());
			reader = new BufferedReader(stream);
			
			return loadReaderAndIndex(writer, outputWriter, reader);
		} finally {
			if ( null != reader ) reader.close();
			if ( null != stream) stream.close();
		}
	}

	private int loadReaderAndIndex(IndexWriter writer, Writer outputWriter, BufferedReader reader) throws IOException, CorruptIndexException {
		String line = null;
		int linesAddded = 0;
		while((line=reader.readLine())!=null) {
			line = line.trim();
			if (line.length() == 0) continue;
			writer.addDocument( createDocument(line) );	
		}
		if ( null != outputWriter) outputWriter.append(
			"----------\n</BR>Total  Keywords Loaded :  ").append(
				new Integer(linesAddded).toString());
		return linesAddded;
	} 
	
	public static final void fastSplit(final Collection<String> result,
			final String text, final char separator) {

		if (text == null)
			return;
		if (text.length() == 0)
			return;

		int index1 = 0;
		int index2 = text.indexOf(separator);

		if (index2 >= 0) {
			String token = null;
			while (index2 >= 0) {
				token = text.substring(index1, index2);
				result.add(token);
				index1 = index2 + 1;
				index2 = text.indexOf(separator, index1);
				if (index2 < 0)
					index1--;
			}

			if (index1 < text.length() - 1) {
				result.add(text.substring(index1 + 1));
			}

		} else {
			result.add(text);
		}
	}		
    
}
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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.thirdparty.guava.common.base.Splitter;

public class DictionaryCreation {
	
	private static DictionaryCreation dict = null;
	public static DictionaryCreation getInstance() {
		if ( null != dict) return dict;
		synchronized (Dictionary.class) {
			if ( null != dict) return dict;
			dict = new DictionaryCreation();
		}
		return dict;
	}
	
	Map<String, Integer> content = new HashMap<String, Integer>();
	static int counter = 1;
	public void addEntry(String entry) {
		if (content.containsKey(entry)) return;
		
		Integer entryIndex = counter++;
		content.put(entry, entryIndex);
	}
	
	public void buildDictionary(String fileUrl, List<Integer> colIndexes, 
			char separator, char appender, boolean ignoreHeader, int maxLines, IColumnCallback callbackF) 
			throws MalformedURLException, IOException {

		InputStreamReader stream = null;
		BufferedReader reader = null;

		Splitter splitter = Splitter.on(separator);
		
		try {
			URL url = new URL(fileUrl);
			stream = new InputStreamReader (url.openStream());
			reader = new BufferedReader(stream);
			
			int maxColIndexes = -1;
			for (Integer aColIndex : colIndexes) {
				if ( maxColIndexes  < aColIndex ) maxColIndexes  = aColIndex;
			}
			
			String line = null;
			int colIndex = 0;
			StringBuilder anEntry = new StringBuilder();
			String[] fields = new String[maxColIndexes+1];
			
			Set<Integer> colIndexesS = new HashSet<Integer>(colIndexes);
			colIndexesS.addAll(colIndexes);
			boolean isFirst = true;
			int lineNo = -1;
			
			while((line=reader.readLine())!=null) {
				lineNo++;
				if ( ignoreHeader && lineNo == 0) continue;

				if ( maxLines >= 0 && lineNo > maxLines) break;
				
				if (line.length() == 0) continue;
				char first=line.charAt(0);
				switch (first) {
					case ' ' : case '\n' : case '#' :  // skip blank & comment lines
					continue;
				}

				colIndex = -1;
				anEntry.delete(0, anEntry.capacity());
				Arrays.fill(fields, null);
				
				for (String colVal : splitter.split(line)) {
					colIndex++;		
					if ( colIndex > maxColIndexes) break;
					if ( ! colIndexesS.contains(colIndex) ) continue;
					
					fields[colIndex] = colVal;
				}
				
				if ( null != callbackF) fields = callbackF.massageColumns(fields);
				
				isFirst = true;
				for (Integer seqIndex : colIndexes) {
					if ( isFirst ) {
						isFirst = false;
					} else {
						anEntry.append(appender);
					}
					anEntry.append(fields[seqIndex]);
				}
				
				addEntry(anEntry.toString());
			}
		} finally {
			if ( null != reader ) try { reader.close(); } catch (IOException ex) {ex.printStackTrace(System.err);}
			if ( null != stream) try { stream.close(); } catch (IOException ex) {ex.printStackTrace(System.err);}
		}		
	
	}	
	
	public void print(Writer writer) throws IOException {
		boolean isFirst = true;
		System.out.println("Total entries :" + content.size());
		for (String entry : content.keySet()) {
			if ( isFirst ) isFirst = false;
			else writer.append('\n');
			
			writer.append(entry).append('\t').append(content.get(entry).toString());
		}
	}
}

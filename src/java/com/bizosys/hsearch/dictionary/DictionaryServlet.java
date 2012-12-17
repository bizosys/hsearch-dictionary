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
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class DictionaryServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {
		this.doProcess(req, res);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {
		this.doProcess(req, res);
	}

	private void doProcess(HttpServletRequest req, HttpServletResponse res)
			throws ServletException, IOException {

		res.setContentType("text/html");
		res.setCharacterEncoding(req.getCharacterEncoding());

		/**
		 * Store all the parameters in the Sensor request object
		 */
		@SuppressWarnings("unchecked")
		Enumeration<String> reqKeys = req.getParameterNames();

		String action = null;
		String query = null;
		while (reqKeys.hasMoreElements()) {
			String key = (String) reqKeys.nextElement();
			String value = req.getParameter(key);
			if ("action".equals(key))
				action = value;
			else if ("query".equals(key))
				query = value;

		}

		/**
		 * Initiate the sensor response, putting the stamp on it and xsl.
		 */
		PrintWriter out = res.getWriter();

		try {
			if ("load".equals(action)) {
				Dictionary.getInstance().load(query, out);
			} else if ("exact.description".equals(action)) {
				out.append(Dictionary.getInstance().findExact(query));
			} else if ("search.top.description".equals(action)) {
				out.append(Dictionary.getInstance().findTopDocument(query, false));
			} else if ("search.top.word".equals(action)) {
				out.append(Dictionary.getInstance().findTopDocument(query, true));
			} else if ("search.predict".equals(action)) {
				out.append(Dictionary.getInstance().predict(query).toString());
			} else if ("exact.exists".equals(action)) {
				if ( Dictionary.getInstance().containsExact(query) ) out.append("true");
				else out.append("false");
			}

		} catch (Exception ex) {
			res.sendError(HttpServletResponse.SC_EXPECTATION_FAILED, "It is a technical issue. Please contact admin at info@bizosys.com");
		} finally {
			out.flush();
			out.close();
		}
	}
}
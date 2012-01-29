/**
 * (C) 2010-2011 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 * 
 */

package com.taobao.datax.plugins.writer.oraclewriter;

import org.apache.log4j.Logger;

import com.taobao.datax.common.constants.Constants;
import com.taobao.datax.common.exception.ExceptionTracker;
import com.taobao.datax.common.exception.DataExchangeException;

public class OracleWriterJni {
	private Logger logger = Logger.getLogger(OracleWriterJni.class);

	private static OracleWriterJni instance;

	public static synchronized OracleWriterJni getInstance() {
		if (null == instance) {
			instance = new OracleWriterJni();
		}
		return instance;
	}

	private OracleWriterJni() {
		try {
			System.load(Constants.DATAX_LOCATION
					+ "/plugins/writer/oraclewriter/libcharset.so");
			System.load(Constants.DATAX_LOCATION
					+ "/plugins/writer/oraclewriter/libiconv.so.2");
			System.load(Constants.DATAX_LOCATION
					+ "/plugins/writer/oraclewriter/liboraclewriter.so");
		} catch (Exception e) {
			logger.error(ExceptionTracker.trace(e));
			throw new DataExchangeException(e.getCause());
		}
	}

	public native long oracle_dumper_init(String logon, String table,
			String sep, String pre, String post, String dtfmt, String encoding,
			String colorder, String limit, long parallel, long skipindex);

	public native int oracle_dumper_connect(long p);

	public native int oracle_dumper_predump(long p, long flag);

	public native int oracle_dumper_dump(long p, String line);

	public native int oracle_dumper_commit(long p);

	public native int oracle_dumper_finish(long p, long flag);

}

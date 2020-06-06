package com.aei.dbconnector;

import java.util.List;

public interface DBConnector {
	<T>void insert(T obj) throws Exception;
	<T> List<T> select(Class<T> type) throws Exception;
	<T> List<T> selectByFields(Class<T> type, String[] fields, Object[] values) throws Exception;
	<T> void delete(Class<T> type, Object id) throws Exception;
	<T> void dropAll(Class<T> type) throws Exception;
	<T> void update(Class<T> type, T obj) throws Exception;
}

package com.aei.dbconnector;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.IntStream;

public class ResultSetParser {
	
	public static <T extends Object> T fromResultSet(ResultSet resultSet, Class<T> type) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, SQLException {
		Constructor<T> c = type.getConstructor();
		T result = c.newInstance();
		IntStream.range(0, resultSet.getMetaData().getColumnCount())
			.forEach(i->{
				try {
					String name = resultSet.getMetaData().getColumnName(i+1);
					Arrays.stream(type.getMethods()).filter(method-> method.getName().equals("set" + Utils.capitalizeString(name)))
						.findAny().ifPresent(res->{
							try {
								if(res.getParameterTypes()[0].isInstance(new String()))
									res.invoke(result, resultSet.getObject(i+1).toString());
								else {
									if(res.getParameterTypes()[0].equals(Boolean.class)) {
										res.invoke(result, resultSet.getBoolean(i+1));
									}else {
										res.invoke(result, resultSet.getObject(i+1));
									}
								}
							} catch (Exception e) {
								System.err.println(res.getName());
								e.printStackTrace();
							}
						});
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		return result;
	}
}

package com.aei.dbconnector;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;


public class SQLConnector implements DBConnector{
	
	public static class SQLConnectionException extends Exception{

		public SQLConnectionException() {
			super();
			// TODO Auto-generated constructor stub
		}

		public SQLConnectionException(String message, Throwable cause) {
			super(message, cause);
			// TODO Auto-generated constructor stub
		}

		public SQLConnectionException(String message) {
			super(message);
			// TODO Auto-generated constructor stub
		}

		public SQLConnectionException(Throwable cause) {
			super(cause);
			// TODO Auto-generated constructor stub
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 2247108101208643600L;
	}
	
	private Connection connection;
	
	public SQLConnector() {
	}
	
	private final static Set<Class<?>> NUMBER_REFLECTED_PRIMITIVES;
	static {
	    Set<Class<?>> s = new HashSet<>();
	    s.add(byte.class);
	    s.add(short.class);
	    s.add(int.class);
	    s.add(long.class);
	    s.add(float.class);
	    s.add(double.class);
	    NUMBER_REFLECTED_PRIMITIVES = s;
	}

	private static boolean isReflectedAsNumber(Class<?> type) {
	    return Number.class.isAssignableFrom(type) || NUMBER_REFLECTED_PRIMITIVES.contains(type);
	}
	
	public <T> void createDatabase(Class<T> type) throws Exception {
		StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
		query.append(type.getAnnotationsByType(DBTable.class)[0].name());
		query.append(" ( ");
		Arrays.stream(type.getDeclaredFields()).forEach(field->{
			boolean pk = false;
			boolean ai = false;
			try {
				DBField annotation = field.getAnnotationsByType(DBField.class)[0];
				ai = annotation.autoIncreasment();
				pk = annotation.id();
			}catch(Exception e) {}
			query.append(field.getName());
			System.out.println(field.getName() + " " + field.getType().getName());
			if(field.getType().equals(Double.class) || field.getType().equals(Float.class)) {
				query.append(" DECIMAL");
			}else if(field.getType().equals(Long.class)){
				query.append(" LONG");
			}else if(isReflectedAsNumber(field.getType()) || ai) {
				query.append(" INT");
			}else if(field.getType().equals(String.class)) {
				query.append(" VARCHAR(255)");
			}else if(field.getType().isEnum()) {
				query.append(" VARCHAR(255)");
			}
			if(ai) {
				query.append(" AUTO_INCREMENT");
			}
			if(pk)
				query.append(" PRIMARY KEY");
			query.append(",");
		});
		query.deleteCharAt(query.length()-1);
		query.append(")");
		System.out.println(query.toString());
		connection.prepareStatement(query.toString()).execute();
	}
	
	public void connect(String url) throws SQLConnectionException{
		try {
			connection = DriverManager.getConnection(url);
		}catch(Exception e) {
			throw new SQLConnectionException(e);
		}
		
	}
	
	public void close() {
		try {
			connection.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void connect(String url, String username, String password) throws SQLConnectionException{
		try {
			connection = DriverManager.getConnection(url,username,password);
		}catch(Exception e) {
			throw new SQLConnectionException(e);
		}
		
	}
	
	public <T extends Object> List<T> selectQuery(String query, Class<T> type, String... values) throws SQLException{
		List<T> result = new ArrayList<>();
		PreparedStatement stmt = connection.prepareStatement(query);
		IntStream.range(0, values.length).forEach(i->{
			try {
				stmt.setString(i+1, values[i]);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		ResultSet resultSet = stmt.executeQuery();
		while(resultSet.next()) {
			try {
				result.add(ResultSetParser.fromResultSet(resultSet, type));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public <T extends Object> List<T> select(Class<T> type) throws SQLException{
		List<T> result = new ArrayList<>();
		String query = "SELECT * FROM " + type.getAnnotationsByType(DBTable.class)[0].name();
		PreparedStatement stmt = connection.prepareStatement(query);
		ResultSet resultSet = stmt.executeQuery();
		while(resultSet.next()) {
			try {
				result.add(ResultSetParser.fromResultSet(resultSet, type));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	public <T extends Object>void insert(T obj) throws SQLException{
		StringBuilder query = new StringBuilder("INSERT INTO ");
		query.append(obj.getClass().getAnnotationsByType(DBTable.class)[0].name());
		query.append(" (");
		StringBuilder values = new StringBuilder("(");
		Arrays.stream(obj.getClass().getDeclaredFields()).forEach(field->{
			try {
				DBField fieldAnnotation = field.getAnnotationsByType(DBField.class)[0];
				if(fieldAnnotation.readOnly()) return;
			}catch(Exception e) {}
			if(Modifier.isPrivate(field.getModifiers())){
				field.setAccessible(true);
			}
			try {
				query.append(field.getName());
				query.append(',');
				values.append("?,");
				
			}catch(Exception e) {
				
			}
		});
		query.deleteCharAt(query.length()-1);
		values.deleteCharAt(values.length()-1);
		query.append(')');
		values.append(')');
		query.append(" VALUES ");
		query.append(values);
				
		PreparedStatement statement = connection.prepareStatement(query.toString());
		Counter counter = new Counter(1);
		IntStream.range(0, obj.getClass().getDeclaredFields().length).forEach(i->{
			Field field = obj.getClass().getDeclaredFields()[i];
			try {
				DBField fieldAnnotation = field.getAnnotationsByType(DBField.class)[0];
				if(fieldAnnotation.readOnly()) return;
			}catch(Exception e) {}
			if(Modifier.isPrivate(field.getModifiers())){
				field.setAccessible(true);
			}
			try {
				if(field.getType().equals(Boolean.class))
					statement.setString(counter.getNum(), (Boolean)field.get(obj) ? "1" : "0");
				else
					statement.setString(counter.getNum(), field.get(obj).toString());
				counter.tick();
			}catch(Exception e) {
				
			}
		});
		System.out.println(statement.toString());
		statement.execute();
	}
	
	public boolean isConnected() {
		try {
			if(connection == null || connection.isClosed())
				return false;
		} catch (SQLException e) {
			return false;
		}
		return true;
	}

	@Override
	public <T> void delete(Class<T> type, Object id) throws SQLException  {
		StringBuilder query = new StringBuilder("DELETE FROM ");
		query.append(type.getAnnotationsByType(DBTable.class)[0].name());
		String field = Arrays.stream(type.getDeclaredFields())
				.filter(f->{
					try {
						return f.getAnnotationsByType(DBField.class)[0].id();
					}catch(Exception e) { return false; }
				})
				.findAny().get().getName();
		query.append(" WHERE ");
		query.append(field);
		query.append(" = ");
		query.append('?');
		
		PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
		preparedStatement.setString(1, id.toString());
		preparedStatement.execute();
	}

	@Override
	public <T> void dropAll(Class<T> type) throws Exception {
		// TODO Auto-generated method stub
		String query = "TRUNCATE " + type.getAnnotationsByType(DBTable.class)[0].name();
		PreparedStatement statement = connection.prepareStatement(query);
		statement.execute();
	}

	@Override
	public <T> List<T> selectByFields(Class<T> type, String[] fields, Object[] values) throws Exception {
		List<T> result = new ArrayList<>();
		StringBuilder query = new StringBuilder("SELECT * FROM ");
		query.append(type.getAnnotationsByType(DBTable.class)[0].name());
		query.append(" WHERE ");
		IntStream.range(0, fields.length).forEach(i->{
			query.append(fields[i]);
			query.append(" = ?,");
		});
		query.deleteCharAt(query.length()-1);
		PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
		IntStream.range(0, values.length).forEach(i->{
			try {
				preparedStatement.setString(i+1, values[i].toString());
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		ResultSet resultSet = preparedStatement.executeQuery();
		while(resultSet.next()) {
			try {
				result.add(ResultSetParser.fromResultSet(resultSet, type));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}
	
	private class Counter{
		private int num;
		public Counter(int num) {this.num = num;}
		public int getNum() {return num;}
		public void tick() {num++;}
	}

	@Override
	public <T> void update(Class<T> type, T obj) throws Exception {
		StringBuilder idName = new StringBuilder();
		StringBuilder id = new StringBuilder();
		ArrayList<String> values = new ArrayList<>();
		StringBuilder query = new StringBuilder("UPDATE ");
		query.append(type.getAnnotationsByType(DBTable.class)[0].name());
		query.append(" SET ");
		Arrays.stream(type.getDeclaredFields()).filter(field->{
			try {
				if(field.getAnnotationsByType(DBField.class)[0].id()) {
					if(Modifier.isPrivate(field.getModifiers()))
						field.setAccessible(true);
					id.append(field.get(obj).toString());
					idName.append(field.getName());
				}
				return !(field.getAnnotationsByType(DBField.class)[0].id() || field.getAnnotationsByType(DBField.class)[0].readOnly());
			}catch(Exception e) {
				return true;
			}
		}).forEach(field->{
			if(Modifier.isPrivate(field.getModifiers()))
				field.setAccessible(true);
			try {
				if(field.getType().equals(Boolean.class))
					values.add((Boolean)field.get(obj) ? "1" : "0");
				else
					values.add(field.get(obj).toString());
				query.append(field.getName());
				query.append(" = ?,");
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		query.deleteCharAt(query.length()-1);
		query.append(" WHERE ");
		query.append(idName.toString());
		query.append(" = ?");
		System.out.print("Query: " + query.toString() + " (");
		PreparedStatement preparedStatement = connection.prepareStatement(query.toString());
		IntStream.range(0, values.size()).forEach(i->{
			try {
				System.out.print(values.get(i) + ", ");
				preparedStatement.setString(i+1, values.get(i));
			} catch (SQLException e) {
				e.printStackTrace();
			}
		});
		System.out.print(id);
		System.out.println(")");
		preparedStatement.setString(values.size()+1, id.toString());
		preparedStatement.execute();
	}
}

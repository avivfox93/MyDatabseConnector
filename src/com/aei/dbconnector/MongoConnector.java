package com.aei.dbconnector;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class MongoConnector implements DBConnector{

	private MongoClient client;
	private String databaseName = "test";
	
	public MongoConnector() {
		
	}
	
	public MongoConnector(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public void connect(String uri) {
		client = new MongoClient(new MongoClientURI(uri));
	}
	
	public void close() {
		if(client != null)
			client.close();
	}
	
	public void setDatabaseName(String db) {
		this.databaseName = db;
	}
	
	public boolean isConnected() {
		return client != null;
	}
	
	public <T> List<T> select(Class<T> type){
		Constructor<T> c;
		try {
			c = type.getConstructor();
		} catch(Exception e) {
			return null;
		}
		List<T> result = new ArrayList<T>();
		MongoCollection<Document> collection = client.getDatabase(databaseName)
				.getCollection(type.getAnnotationsByType(DBTable.class)[0].name());
		MongoCursor<Document> cursor = collection.find().iterator();
		try {
			while(cursor.hasNext()) {
				T obj = c.newInstance();
				Document document = cursor.next();
				document.keySet().forEach(key->{
					if(key.equals("_id")) {
						Arrays.stream(type.getDeclaredFields())
							.filter(f->f.getAnnotationsByType(DBField.class).length > 0)
							.filter(f->f.getAnnotationsByType(DBField.class)[0].id())
							.findAny().ifPresent(f->{
								try {
									type.getMethod("set" + Utils.capitalizeString(f.getName()), String.class)
										.invoke(obj,document.getObjectId(key).toHexString());
								} catch (Exception e) {
									System.err.println("set" + Utils.capitalizeString(f.getName()));
									e.printStackTrace();
								}
							});
					}
					Arrays.stream(type.getMethods())
						.filter(method->method.getName().equals("set" + Utils.capitalizeString(key)))
						.findAny().ifPresent(method->{
							try {
								method.invoke(obj, document.get(key));
							} catch (Exception e) {}
						});
				});
				result.add(obj);
			}
		}catch(Exception e) {
			
		}finally{
			cursor.close();
		}
		return result;
	}
	
	public <T>void insert(T obj){
		MongoCollection<Document> collection = client
				.getDatabase(databaseName)
				.getCollection(obj.getClass().getAnnotationsByType(DBTable.class)[0].name());
		Document document = DocumentConverter.toDocument(obj);
		collection.insertOne(document);
	}

	@Override
	public <T> void delete(Class<T> type, Object id) {
		MongoCollection<Document> collection = client
				.getDatabase(databaseName)
				.getCollection(type.getAnnotationsByType(DBTable.class)[0].name());
		BasicDBObject query = new BasicDBObject();
		query.put("_id", (id instanceof String) ? new ObjectId((String)id) : id);
		collection.findOneAndDelete(query);
	}
	
	public <T> void dropAll(Class<T> type) {
		MongoCollection<Document> collection = client
				.getDatabase(databaseName)
				.getCollection(type.getAnnotationsByType(DBTable.class)[0].name());
		collection.drop();
	}

	@Override
	public <T> List<T> selectByFields(Class<T> type, String[] fields, Object[] values) throws Exception {
		BasicDBObject query = new BasicDBObject();
		IntStream.range(0, fields.length).forEach(i->{
			query.put(fields[i],values[i]);
		});
		Constructor<T> c;
		try {
			c = type.getConstructor();
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
		List<T> result = new ArrayList<T>();
		MongoCollection<Document> collection = client.getDatabase(databaseName)
				.getCollection(type.getAnnotationsByType(DBTable.class)[0].name());
		MongoCursor<Document> cursor = collection.find(query).iterator();
		try {
			while(cursor.hasNext()) {
				T obj = c.newInstance();
				Document document = cursor.next();
				document.keySet().forEach(key->{
					if(key.equals("_id")) {
						Arrays.stream(type.getDeclaredFields())
							.filter(f->f.getAnnotationsByType(DBField.class).length > 0)
							.filter(f->f.getAnnotationsByType(DBField.class)[0].id())
							.findAny().ifPresent(f->{
								try {
									type.getMethod("set" + Utils.capitalizeString(f.getName()),String.class)
										.invoke(obj,document.getObjectId(key).toHexString());
								} catch (Exception e) {
									e.printStackTrace();
								}
							});
					}
					Arrays.stream(type.getMethods())
						.filter(method->method.getName().equals("set" + Utils.capitalizeString(key)))
						.findAny().ifPresent(method->{
							try {
								method.invoke(obj, document.get(key));
							} catch (Exception e) {}
						});
				});
				result.add(obj);
			}
		}catch(Exception e) {
			
		}finally{
			cursor.close();
		}
		return result;
	}

	@Override
	public <T> void update(Class<T> type, T obj) throws Exception {
		Field id = Arrays.stream(type.getDeclaredFields()).filter(field->{
			try {
			return field.getAnnotationsByType(DBField.class)[0].id();
			}catch(Exception e) {
				return false;
			}
		}).findAny().get();
		if(Modifier.isPrivate(id.getModifiers()))
			id.setAccessible(true);
		id.get(obj);
		BasicDBObject query = new BasicDBObject();
		query.put("_id", new ObjectId(id.get(obj).toString()));
		MongoCollection<Document> collection = client.getDatabase(databaseName)
				.getCollection(type.getAnnotationsByType(DBTable.class)[0].name());
		Document doc = DocumentConverter.toDocument(obj);
		collection.replaceOne(query, doc);
	}

}

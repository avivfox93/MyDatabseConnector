package com.aei.dbconnector;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import org.bson.Document;

public class DocumentConverter {
	public static <T extends Object>Document toDocument(T obj){
		Document result = new Document();
		Arrays.stream(obj.getClass().getDeclaredFields()).forEach(field->{
			try {
				DBField fieldAnnotation = field.getAnnotationsByType(DBField.class)[0];
				if(fieldAnnotation.readOnly()) return;
			}catch(Exception e) {}
			if(Modifier.isPrivate(field.getModifiers())){
				field.setAccessible(true);
			}
			try {
				result.append(field.getName(), field.get(obj));
			}catch(Exception e) {}
		});
		return result;
	}
}

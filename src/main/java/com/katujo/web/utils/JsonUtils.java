
//Namespace
package com.katujo.web.utils;

//Imports
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * JSON util methods that makes it easier to work with GSON/JSON in java.
 * @author Johan Hertz
 *
 */
public class JsonUtils
{
	//The map that holds the translation from SQL column format to JSON field name
	//<SQL_COLUMN, JSON_FIELD>
	private static final ConcurrentHashMap<String, String> createJsonObjectColumnTranslator = new ConcurrentHashMap<String, String>();	
	
	//Database types used when creating JSON objects and arrays	
	private static enum DatabaseTypes
	{
		BOOLEAN,
		DATE,
		DOUBLE,
		INTEGER,
		LONG,
		STRING,		
		TIMESTAMP
	}
	
	/**
	 * Get the member value as an object.
	 * @param element
	 * @param member
	 * @return
	 * @throws Exception
	 */	
	public static Object get(JsonElement element, String member) throws Exception
	{
		//Check if null
		if(element == null)
			return null;
		
		//Check if object
		if(!element.isJsonObject())
			throw new Exception("Element is not a JSON object");
		
		//Get the object value for the member
		return get(element.getAsJsonObject(), member);
	}
	
	/**
	 * Get the member value as an object.
	 * @param obj
	 * @param member
	 * @return
	 * @throws Exception
	 */
	public static Object get(JsonObject obj, String member) throws Exception
	{
		//The value is null
		if(obj.get(member) == null || obj.get(member).isJsonNull())
			return null;
		
		//Get the primitive
		JsonPrimitive primitive = obj.get(member).getAsJsonPrimitive();
		
		//Boolean
		if(primitive.isBoolean())
			return primitive.getAsBoolean();
		
		//Number/Double
		if(primitive.isNumber())
			return primitive.getAsDouble();
		
		//String
		if(primitive.isString())
			return primitive.getAsString();
		
		//Could not find a matching type
		throw new Exception("The member type is not regonised");
	}	
	
	/**
	 * Get the date from a number member.
	 * @param element
	 * @param member
	 * @return
	 * @throws Exception
	 */
	public static Date getDate(JsonElement element, String member) throws Exception
	{
		//Check if null
		if(element == null)
			return null;
		
		//Check if object
		if(!element.isJsonObject())
			throw new Exception("Element is not a JSON object");
		
		//Get the value
		return getDate(element.getAsJsonObject(), member);
	}
	
	/**
	 * Get the date from a number member.
	 * @param obj
	 * @param member
	 * @return
	 * @throws Exception
	 */
	public static Date getDate(JsonObject obj, String member) throws Exception
	{
		//Get the getObject
		Object getObj = get(obj, member);
		
		//Check if getObject is null
		if(getObj == null)
			return null;
		
		//Check if getObject is not a Double
		if(!(getObj instanceof Double))
			throw new Exception("Only number members can be cast to Date");
		
		//Create a date from the number value
		return new Date(((Double) getObj).longValue());
	}
	
	/**
	 * Get a number property from a JSON element as double.
	 * @param element
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public static Double getDouble(JsonElement element, String property) throws Exception
	{
		//Check if null
		if(element == null)
			return null;
		
		//Check if object
		if(!element.isJsonObject())
			throw new Exception("Element is not a JSON object");
		
		//Get the value
		return getDouble(element.getAsJsonObject(), property);				
	}
	 		
	/**
	 * Get a number property from a JSON object as double.
	 * @param obj
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public static Double getDouble(JsonObject obj, String property) throws Exception
	{
		//Get the getObject
		Object getObj = get(obj, property);
		
		//Check if getObject is null
		if(getObj == null)
			return null;	
		
		//Return double if double
		if(getObj instanceof Double)
			return (Double) getObj;
		
		//Return the float as double
		if(getObj instanceof Float)
			return ((Float) getObj).doubleValue();
		
		//Return the integer as double
		if(getObj instanceof Integer)
			return ((Integer) getObj).doubleValue();	
		
		//Could not find a matching number type
		throw new Exception("Type is not recognised as number " + getObj.getClass().getSimpleName());
	}
	
	/**
	 * Get a number property from a JSON element as float.
	 * @param element
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public static Float getFloat(JsonElement element, String property) throws Exception
	{
		//Check if null
		if(element == null)
			return null;
		
		//Check if object
		if(!element.isJsonObject())
			throw new Exception("Element is not a JSON object");
		
		//Get the value
		return getFloat(element.getAsJsonObject(), property);				
	}	
	
	/**
	 * Get a number property from a JSON object as float.
	 * @param obj
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public static Float getFloat(JsonObject obj, String property) throws Exception
	{
		//Get the getObject
		Object getObj = get(obj, property);
		
		//Check if getObject is null
		if(getObj == null)
			return null;	
		
		//Return float as double
		if(getObj instanceof Double)
			return ((Double) getObj).floatValue();
		
		//Return the float as float
		if(getObj instanceof Float)
			return (Float) getObj;
		
		//Return the integer as float
		if(getObj instanceof Integer)
			return ((Integer) getObj).floatValue();	
		
		//Could not find a matching number type
		throw new Exception("Type is not recognised as number " + getObj.getClass().getSimpleName());
	}	
	
	/**
	 * Get a number property from a JSON object as integer.
	 * @param obj
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public static Integer getInt(JsonObject obj, String property) throws Exception
	{
		//Get the getObject
		Object getObj = get(obj, property);
		
		//Check if getObject is null
		if(getObj == null)
			return null;	
		
		//Return double as integer
		if(getObj instanceof Double)
			return ((Double) getObj).intValue();
		
		//Return the float as integer
		if(getObj instanceof Float)
			return ((Float) getObj).intValue();
		
		//Return the integer as integer
		if(getObj instanceof Integer)
			return (Integer) getObj;	
		
		//Could not find a matching number type
		throw new Exception("Type is not recognised as number " + getObj.getClass().getSimpleName());
	}	
	
	/**
	 * Get a string property from a JSON element.
	 * <p>
	 * If the property is not of string type the toString() method will be called on the object.
	 * </p> 
	 * @param element
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public static String getString(JsonElement element, String property) throws Exception
	{
		//Check if null
		if(element == null)
			return null;
		
		//Check if object
		if(!element.isJsonObject())
			throw new Exception("Element is not a JSON object");
		
		//Get the value
		return getString(element.getAsJsonObject(), property);		
	}
	
	/**
	 * Get a string property from a JSON object.
	 * <p>
	 * If the property is not of string type the toString() method will be called on the object.
	 * </p>
	 * @param obj
	 * @param property
	 * @return
	 * @throws Exception
	 */
	public static String getString(JsonObject obj, String property) throws Exception
	{
		//Get the getObject
		Object getObj = get(obj, property);
		
		//Check if getObject is null
		if(getObj == null)
			return null;	
		
		//Check if string
		if(getObj instanceof String)
			return (String) getObj;
		
		//Return the toString result for the object
		return getObj.toString();		
	}
	
	/**
	 * Check if the object member is true or false.
	 * @param element
	 * @param member
	 * @return
	 * @throws Exception
	 */
	public static boolean is(JsonElement element, String member) throws Exception
	{
		//Return false if element not set
		if(element == null)
			return false;
		
		//Call the is function with the element as an object
		if(element.isJsonObject())
			return is(element.getAsJsonObject(), member);
		
		//The is function can only handle JSON objects
		throw new Exception("JSON element is not a JSON object");
	}
	
	/**
	 * Check if the object member is true or false.
	 * 
	 * <table border="1">
	 * 	<tr><td><b>Type</b></td><td><b>Value</b></td><td><b>Returns</b></td></tr>
	 * 	<tr><td>Array</td><td>any but null</td><td>true</td></tr>
	 * 	<tr><td>Array</td><td>null</td><td>false</td></tr>
	 * 	<tr><td>Boolean</td><td>true</td><td>true</td></tr>
	 * 	<tr><td>Boolean</td><td>false</td><td>false</td></tr>
	 * 	<tr><td>Number</td><td>Any but 0</td><td>true</td></tr>
	 * 	<tr><td>Number</td><td>0</td><td>false</td></tr>
	 * 	<tr><td>String</td><td>Any but empty</td><td>true</td></tr>
	 * 	<tr><td>String</td><td>empty</td><td>false</td></tr>
	 * 	<tr><td>Object</td><td>any but null</td><td>true</td></tr>
	 *  <tr><td>Object</td><td>null</td><td>false</td></tr>
	 *  <tr><td>*</td><td>obj or member is null or JSON null</td><td>false</td></tr>
	 * </table>
	 * 
	 * @param obj
	 * @param member
	 * @return
	 * @throws Exception
	 */
	public static boolean is(JsonObject obj, String member) throws Exception
	{
		//If object is not set return false
		if(obj == null || obj.isJsonNull())
			return false;
		
		//If the object does not have the member return false
		if(!obj.has(member))
			return false;
			
		//If the member is null return false
		if(obj.get(member) == null || obj.get(member).isJsonNull())
			return false;	
		
		//If the member is an object or an array return true (if set)
		if(obj.get(member).isJsonObject() || obj.get(member).isJsonArray())
			return true;
		
		//Get the object
		Object get = get(obj, member);
		
		//If the get object is null return false
		if(get == null)
			return false;
		
		//If this is a boolean return as is 
		if(get instanceof Boolean)
			return (Boolean) get;
		
		//Double: 0 is false all other number are true
		if(get instanceof Double)
			return !((Double) get == 0); 		
		
		//String: empty string is false, all other are true
		if(get instanceof String)
			return !"".equals(get); 
		
		//Could not find a matching type
		throw new Exception("The member type is not regonised");		
	}	
	
	/**
	 * Create a JSON array of JSON objects to hold the data in 
	 * the result set.
	 * @param result
	 * @return
	 * @throws Exception
	 */
	public static JsonArray createJsonArray(ResultSet result) throws Exception
	{
		//Try to create the data
		try
		{
			//Create the JSON array to hold the data
			JsonArray data = new JsonArray();
			
			//Get the meta data
			ResultSetMetaData meta = result.getMetaData();
						
			//Get the column types and the field names
			DatabaseTypes[] columnTypes = getColumnTypes(meta);
			String[] fieldNames = getFieldNames(meta);			
			
			//Read the result into the data
			while(result.next())
				data.add(createJsonObject(result, columnTypes, fieldNames));
			
			//Return the data
			return data;			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to create JSON array from result set", ex);
		}
	}
	
	/**
	 * Create a JSON object from a result set row.
	 * @param result
	 * @return
	 * @throws Exception
	 */
	public static JsonObject createJsonObject(ResultSet result) throws Exception
	{
		//Get the meta data
		ResultSetMetaData meta = result.getMetaData();
		
		//Create the JSON object
		return createJsonObject(result, getColumnTypes(meta), getFieldNames(meta));		
	}
	
	/**
	 * Create a JSON object from a result set row using the column types and the field names.
	 * @param result
	 * @param columnTypes
	 * @param fieldNames
	 * @return
	 * @throws Exception
	 */
	public static JsonObject createJsonObject(ResultSet result, DatabaseTypes[] columnTypes, String[] fieldNames) throws Exception
	{
		//Try to create the JSON object
		try
		{
			//Create the JSON object
			JsonObject obj = new JsonObject();
							
			//Read the data into the object
			for(int i=0; i<fieldNames.length; i++)
			{															
				//Add the data to the object
				if(DatabaseTypes.STRING == columnTypes[i]) obj.addProperty(fieldNames[i], result.getString(i+1));
				else if(DatabaseTypes.DOUBLE == columnTypes[i]) obj.addProperty(fieldNames[i], getDouble(result, i+1));
				else if(DatabaseTypes.INTEGER == columnTypes[i]) obj.addProperty(fieldNames[i], getInteger(result, i+1));
				else if(DatabaseTypes.BOOLEAN == columnTypes[i]) obj.addProperty(fieldNames[i], getBoolean(result, i+1));
				else if(DatabaseTypes.LONG == columnTypes[i]) obj.addProperty(fieldNames[i], getLong(result, i+1));
				else if(DatabaseTypes.DATE == columnTypes[i]) obj.addProperty(fieldNames[i], getDate(result, i+1));											
				else if(DatabaseTypes.TIMESTAMP == columnTypes[i]) obj.addProperty(fieldNames[i], getTimestamp(result, i+1));
				else throw new Exception("No mapping made for column type " + columnTypes[i]);
			}
			
			//Return the JSON object
			return obj;			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to create the JSON object from the current result set row", ex);
		}	
	}	
	
	/**
	 * Get the column types.
	 * @param meta
	 * @return
	 * @throws Exception
	 */
	private static DatabaseTypes[] getColumnTypes(ResultSetMetaData meta) throws Exception
	{
		//Create the column index(used in catch)
		int column = 0;
		
		//Try to get the column types
		try
		{
			//Create the types array
			DatabaseTypes[] types = new DatabaseTypes[meta.getColumnCount()];
			
			//Populate the types
			for(column=0; column<types.length; column++)
			{
				//Get the column type
				String type = meta.getColumnClassName(column+1);
				
				//If class column type not found use database column type instead
				if(type == null)
					type = meta.getColumnTypeName(column+1);
				
				//Type could not be set (will throw error)
				if(type == null)
					type = "NOT_SET";
				
				//Change to type long if field is BIG_DECIMAL and does not have a scale (Oracle number fix)
				if(meta.getScale(column+1) == 0 && BigDecimal.class.getName().equals(type))
					type = Long.class.getName();					
				
				//Add the types to the array
				if(String.class.getName().equals(type)) types[column] = DatabaseTypes.STRING;
				else if(Object.class.getName().equals(type)) types[column] = DatabaseTypes.STRING; //This is for null values like SELECT NULL AS MY_COLUMN FROM ...
				else if(Double.class.getName().equals(type)) types[column] = DatabaseTypes.DOUBLE;
				else if(Float.class.getName().equals(type)) types[column] = DatabaseTypes.DOUBLE;
				else if(BigDecimal.class.getName().equals(type) || type.contains("BINARY_DOUBLE")) types[column] = DatabaseTypes.DOUBLE;
				else if(Integer.class.getName().equals(type)) types[column] = DatabaseTypes.INTEGER;
				else if(Long.class.getName().equals(type) || BigInteger.class.getName().equals(type)) types[column] = DatabaseTypes.LONG;				
				else if(java.sql.Timestamp.class.getName().equals(type)) types[column] = DatabaseTypes.TIMESTAMP;
				else if(java.sql.Time.class.getName().equals(type)) types[column] = DatabaseTypes.TIMESTAMP; //TODO: Implement the time database type
				else if(java.sql.Date.class.getName().equals(type)) types[column] = DatabaseTypes.DATE;
				else if(type != null && (type.toUpperCase().endsWith(".CLOB") || type.toUpperCase().endsWith(".ORACLECLOB") || "BINARY".equals(meta.getColumnTypeName(column+1)))) types[column] = DatabaseTypes.STRING;
				else if(Boolean.class.getName().equals(type)) types[column] = DatabaseTypes.BOOLEAN;
				else throw new Exception("There is no mapping for type: " + type);												
			}
						
			//Return the types
			return types;
		}
		
		//Failed
		catch(Exception ex)
		{
			//Create the column name 
			String name = "";
			
			//Try to get the column name
			try{name = meta.getColumnLabel(column+1);} catch(Throwable t) {}
			
			//Throw the exception
			throw new Exception("Failed to get the column types from the meta data (failed on column " + name + " )", ex);
		}
	}
	
	/**
	 * Get the field names for the columns.
	 * @param meta
	 * @return
	 * @throws Exception
	 */
	private static String[] getFieldNames(ResultSetMetaData meta) throws Exception
	{
		//Try to get the field names
		try
		{
			//Create the names array
			String[] fieldNames = new String[meta.getColumnCount()];
			
			//Populate the field names
			for(int i=0; i<fieldNames.length; i++)
				fieldNames[i] = columnToField(meta.getColumnLabel(i+1));
			
			//Return the field names
			return fieldNames;		
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get the field names", ex);
		}
	}
	
	/**
	 * Translate a column name to a JSON field name.
	 * @param column
	 * @return
	 */
	private static String columnToField(String column)
	{
		//Get the field name
		String field = createJsonObjectColumnTranslator.get(column);
		
		//Create the field if not set
		if(field == null)
		{
			//Split the column
			String[] columnSplit = column.split("_");
			
			//Create the string builder to hold the filed
			StringBuilder builder = new StringBuilder();

			//Create the name in the builder
			for(int y=0; y<columnSplit.length; y++)
				if(y==0)
					builder.append(columnSplit[y].toLowerCase());
				else if(columnSplit[y].length() <= 2)
					 builder.append(columnSplit[y]);
				else  builder.append(columnSplit[y].substring(0, 1) + columnSplit[y].substring(1).toLowerCase());
					
			//Set the filed
			field = builder.toString();
			
			//Add the filed to the map
			createJsonObjectColumnTranslator.put(column, field);				
		}		
		
		//Return the filed
		return field;
	}
	
	/**
	 * Get the boolean value if set or null if null.
	 * @param result
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private static Boolean getBoolean(ResultSet result, int index) throws Exception
	{
		//Try to get the value
		try
		{
			//Get the value
			boolean value = result.getBoolean(index);
			
			//Check if null
			if(result.wasNull())
				return null;
			
			//Return the value
			return value;
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get boolean form index " + index, ex);
		}
	}	
	
	/**
	 * Get the timestamp as long for the date if set or null if not set.
	 * @param result
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private static Long getDate(ResultSet result, int index) throws Exception
	{
		//Get the date
		Date date = result.getDate(index);
		
		//Return null if null
		if(date == null)
			return null;
		
		//Return the timestamp if set
		return date.getTime();
	}
	
	/**
	 * Get the double value if set or null if null.
	 * @param result
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private static Double getDouble(ResultSet result, int index) throws Exception
	{
		//Try to get the value
		try
		{
			//Get the value
			double value = result.getDouble(index);
			
			//Check if null
			if(result.wasNull())
				return null;
			
			//Return the value
			return value;
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get double form index " + index, ex);
		}
	}	
	
	/**
	 * Get the integer value if set or null if null.
	 * @param result
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private static Integer getInteger(ResultSet result, int index) throws Exception
	{
		//Try to get the value
		try
		{
			//Get the value
			int value = result.getInt(index);
			
			//Check if null
			if(result.wasNull())
				return null;
			
			//Return the value
			return value;
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get integer form index " + index, ex);
		}
	}
	
	/**
	 * Get the timestamp as long for the timestamp if set or null if not set.
	 * @param result
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private static Long getTimestamp(ResultSet result, int index) throws Exception
	{
		//Get the date
		Timestamp value = result.getTimestamp(index);
		
		//Return null if null
		if(value == null)
			return null;
		
		//Return the timestamp if set
		return value.getTime();
	}	
	
	/**
	 * Get the long value if set or null if null.
	 * @param result
	 * @param index
	 * @return
	 * @throws Exception
	 */
	private static Long getLong(ResultSet result, int index) throws Exception
	{
		//Try to get the value
		try
		{
			//Get the value
			long value = result.getLong(index);
			
			//Check if null
			if(result.wasNull())
				return null;
			
			//Return the value
			return value;
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get long form index " + index, ex);
		}
	}	

}

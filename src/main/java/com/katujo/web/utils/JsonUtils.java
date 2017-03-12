
//Namespace
package com.katujo.web.utils;

//Java imports
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.concurrent.ConcurrentHashMap;



//Google imports
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
	
	/**
	 * Get the member value as an object.
	 * @param element
	 * @param member
	 * @return
	 * @throws Exception
	 */	
	public static Object get(JsonElement element, String member) throws Exception
	{
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
			
			//Read the result into the data
			while(result.next())
				data.add(createJsonObject(result));
			
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
		//Try to create the JSON object
		try
		{
			//Create the JSON object
			JsonObject obj = new JsonObject();
			
			//Get the meta data
			ResultSetMetaData meta = result.getMetaData();
				
			//Read the data into the object
			for(int i=0; i<meta.getColumnCount(); i++)
			{
				//Get the column name
				String column = meta.getColumnLabel(i+1);
				
				//Get the column type
				String type = meta.getColumnClassName(i+1);

				//Change to type long if field is BIG_DECIMAL and does not have a scale (Oracle number fix)
				if(result.getMetaData().getScale(i+1) == 0 && BigDecimal.class.getName().equals(type))
					type = Long.class.getName();				
				
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
											
				//Add the field to the JSON object 
				if(String.class.getName().equals(type)) obj.addProperty(field, result.getString(i+1));
				else if(Double.class.getName().equals(type)) obj.addProperty(field, result.getDouble(i+1));
				else if(BigDecimal.class.getName().equals(type)) obj.addProperty(field, result.getDouble(i+1));
				else if(Integer.class.getName().equals(type)) obj.addProperty(field, result.getInt(i+1));
				else if(Long.class.getName().equals(type) || BigInteger.class.getName().equals(type)) obj.addProperty(field, result.getLong(i+1));				
				else if(java.sql.Timestamp.class.getName().equals(type)) obj.addProperty(field, result.getTimestamp(i+1) == null ? null : result.getTimestamp(i+1).getTime());
				else if(type.toUpperCase().endsWith(".CLOB") || "BINARY".equals(meta.getColumnTypeName(i+1))) obj.addProperty(field, result.getString(i+1));
				else if(java.sql.Date.class.getName().equals(type)) obj.addProperty(field, result.getDate(i+1) == null ? null : result.getDate(i+1).getTime());
				else if(Boolean.class.getName().equals(type)) obj.addProperty(field, result.getBoolean(i+1));				
				else throw new Exception("There is no mapping for type: " + type);
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

}

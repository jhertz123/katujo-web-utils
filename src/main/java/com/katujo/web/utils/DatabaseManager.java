
//Namespace
package com.katujo.web.utils;

//Imports
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Helps with database communication. 
 * @author Johan Hertz
 */
public class DatabaseManager
{	
	//This map holds the data sources
	private final static Map<String, DataSource> dataSources = new ConcurrentHashMap<String, DataSource>();
	
	//The default data source look up that is used when calling methods without the data source specified
	private final String defaultLookup;	
	
	/**
	 * Create the object <b>without</b> a default data source look up set.
	 *
	 */
	public DatabaseManager() 
	{
		this.defaultLookup = null;
	}
	
	/**
	 * Create the object with the default data source look up set.
	 * @param defaultDataSource
	 */
	public DatabaseManager(String defaultDataSource)
	{
		this.defaultLookup = defaultDataSource;
	}
	
	/**
	 * Get the default data source.
	 * @return
	 * @throws Exception
	 */
	protected DataSource getDataSource() throws Exception
	{
		return getDataSource(defaultLookup);
	}
			
	/**
	 * Get the data source using the JNDI name.
	 * <p>
	 * On Tomcat this should be java:comp/env/DATA_SOURCE_NAME
	 * </p>
	 * @param lookup
	 * @return
	 * @throws Exception
	 */
	protected DataSource getDataSource(String lookup) throws Exception
	{
		//Try to get the data source
		try
		{
			//Get the data source from the map
			DataSource source = dataSources.get(lookup);
			
			//Create and add the data source if not set
			if(source == null)
			{							
				//Get the context objects 
				Context initCtx = new InitialContext();
				source = (DataSource) initCtx.lookup(lookup);
					
				//Add the source to the data sources 
				dataSources.put(lookup, source);
			}
			
			//Return the source
			return source;
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get the data source with lookup " + lookup, ex);
		}
	}
	
	/**
	 * Get a connection from the default data source. 
	 * <p>
	 * This connection must be explicitly closed by the caller.
	 * </p> 
	 * @return
	 * @throws Exception
	 */
	public Connection getConnection() throws Exception
	{
		//Try to get a connection
		try {return getDataSource(defaultLookup).getConnection();}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get a connection", ex);
		}		
	}
	
	/**
	 * Get a connection from the named data source. 
	 * <p>
	 * This connection must be explicitly closed by the caller.
	 * </p>
	 * @param name
	 * @return
	 * @throws Exception
	 */
	public Connection getConnection(String name) throws Exception
	{
		//Try to get a connection
		try {return getDataSource(name).getConnection();}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get a connection", ex);
		}		
	}
	
	/**
	 * Load a JSON object from the database using the SQL.
	 * <p>
	 * If no result matched the query null will be returned.
	 * </p>
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	protected JsonObject getObject(String sql) throws Exception
	{
		return getObject(sql, new Object[]{});				 
	}
	
	/**
	 * Load a JSON object from the database using the SQL.
	 * <p>
	 * If no result matched the query null will be returned.
	 * </p>
	 * @param connection
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	protected JsonObject getObject(Connection connection, String sql) throws Exception
	{
		return getObject(sql, new Object[]{});				 
	}	
	
	/**
	 * Load a JSON object from the database using the SQL and the parameter.
	 * <p>
	 * If no result matched the query, null will be returned.
	 * </p>
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */
	protected JsonObject getObject(String sql, List<Object> parameter) throws Exception
	{
		return getObject(sql, parameter.toArray(new Object[parameter.size()]));
	}	
	
	/**
	 * Load a JSON object from the database using the SQL and the parameter.
	 * <p>
	 * If no result matched the query, null will be returned.
	 * </p>
	 * @param connection
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */
	protected JsonObject getObject(Connection connection, String sql, List<Object> parameter) throws Exception
	{
		return getObject(connection, sql, parameter.toArray(new Object[parameter.size()]));
	}		
	
	/**
	 * Load a JSON object from the database using the SQL and the parameter.
	 * <p>
	 * If no result matched the query, null will be returned.
	 * </p>
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */	
	protected JsonObject getObject(String sql, Object parameter) throws Exception
	{
		return getObject(sql, new Object[]{parameter});
	}	
	
	/**
	 * Load a JSON object from the database using the SQL and the parameter.
	 * <p>
	 * If no result matched the query, null will be returned.
	 * </p>
	 * @param connection
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */
	protected JsonObject getObject(Connection connection, String sql, Object parameter) throws Exception
	{
		return getObject(connection, sql, new Object[]{parameter});
	}	
		
	/**
	 * Load a JSON object from the database using the SQL and the parameters.
	 * <p>
	 * If no result matched the query null will be returned.
	 * </p>
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	protected JsonObject getObject(String sql, Object... parameters) throws Exception
	{
		//Fields
		Connection connection = null;
		
		//Try to read data
		try
		{
			//Get a connection
			connection = getConnection();
			
			//Get the JSON object using the connection
			return getObject(connection, sql, parameters);
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get JSON object from database result set", ex);
		}
		
		//Clean up
		finally
		{
			try {connection.close();} catch(Throwable t) {}
		}
	}
	
	/**
	 * Load a JSON object from the database using the SQL and the parameters.
	 * <p>
	 * If no result matched the query null will be returned.
	 * </p>
	 * @param connection
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	protected JsonObject getObject(Connection connection, String sql, Object... parameters) throws Exception
	{
		//Fields
		PreparedStatement statement = null;
		ResultSet result = null;
		
		//Try to read data
		try
		{			
			//Create the statement
			statement = connection.prepareStatement(sql);
			
			//Set the parameters if set 
			if(parameters != null)
			{
				//Set the parameters
				for(int i=0; i<parameters.length; i++)
				{
					//Set the timestamp to avoid being shadowed by below date check
					if(parameters[i] instanceof Timestamp)
						statement.setObject(i+1, (Timestamp) parameters[i]);							
					
					//Convert to SQL date when using java.util.Date parameter
					else if(parameters[i] instanceof Date)
						statement.setObject(i+1, new java.sql.Date(((Date) parameters[i]).getTime()));

					//Set the parameter
					else statement.setObject(i+1, parameters[i]);
				}
			}
			
			//Run the statement
			result = statement.executeQuery();
			
			//Move the result set
			boolean found = result.next();
			
			//No result found
			if(!found) return null;
			
			//Read and return the result
			return JsonUtils.createJsonObject(result);			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get JSON object from database result set", ex);
		}
		
		//Clean up
		finally
		{
			try {result.close();} catch(Throwable t) {}
			try {statement.close();} catch(Throwable t) {}
		}
	}	
	
	/**
	 * Load a JSON array of JSON objects from the database using the SQL.
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(String sql) throws Exception
	{
		return getArray(sql, new Object[]{});
	}
	
	/**
	 * Load a JSON array of JSON objects from the database using the SQL.
	 * @param connection
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(Connection connection, String sql) throws Exception
	{
		return getArray(connection, sql, new Object[]{});
	}
		
	
	/**
	 * Load a JSON array of JSON objects from the database using SQL and the parameter.
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(String sql, List<Object> parameter) throws Exception
	{						
		return getArray(sql, parameter.toArray(new Object[parameter.size()]));
	}
	
	/**
	 * Load a JSON array of JSON objects from the database using SQL and the parameter.
	 * @param connection
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(Connection connection, String sql, List<Object> parameter) throws Exception
	{						
		return getArray(connection, sql, parameter.toArray(new Object[parameter.size()]));
	}		
	
	/**
	 * Load a JSON array of JSON objects from the database using SQL and the parameter.
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(String sql, Object parameter) throws Exception
	{
		return getArray(sql, new Object[]{parameter});
	}
	
	/**
	 * Load a JSON array of JSON objects from the database using SQL and the parameter.
	 * @param connection
	 * @param sql
	 * @param parameter
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(Connection connection, String sql, Object parameter) throws Exception
	{
		return getArray(connection, sql, new Object[]{parameter});
	}	
	
	/**
	 * Load a JSON array of JSON objects from the database using the SQL and the parameters.
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(String sql, Object... parameters) throws Exception
	{
		//Fields
		Connection connection = null;
		
		//Try to read data
		try
		{
			//Get a connection
			connection = getConnection();
			
			//Get the array using the default connection
			return getArray(connection, sql, parameters);			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get JSON array from database result set", ex);
		}
		
		//Clean up
		finally {try {connection.close();} catch(Throwable t) {}}				
	}
	
	/**
	 * Load a JSON array of JSON objects from the database using the SQL and the parameters.
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	protected JsonArray getArray(Connection connection, String sql, Object... parameters) throws Exception
	{
		//Fields
		PreparedStatement statement = null;
		ResultSet result = null;
		
		//Try to read data
		try
		{			
			//Create the statement
			statement = connection.prepareStatement(sql);
			
			//Set the parameters if set 
			if(parameters != null)
			{
				//Set the parameters
				for(int i=0; i<parameters.length; i++)
				{
					//Set the timestamp to avoid being shadowed by below date check
					if(parameters[i] instanceof Timestamp)
						statement.setObject(i+1, (Timestamp) parameters[i]);							
					
					//Convert to SQL date when using java.util.Date parameter
					else if(parameters[i] instanceof Date)
						statement.setObject(i+1, new java.sql.Date(((Date) parameters[i]).getTime()));

					//Set the parameter
					else statement.setObject(i+1, parameters[i]);
				}
			}
			
			//Run the statement
			result = statement.executeQuery();
			
			//Create the array and return it
			return JsonUtils.createJsonArray(result);
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get JSON array from database result set", ex);
		}
		
		//Clean up
		finally
		{
			try {result.close();} catch(Throwable t) {}
			try {statement.close();} catch(Throwable t) {}
		}				
	}	
	
	/**
	 * Execute the SQL with the parameter.
	 * @param sql
	 * @param parameter
	 * @throws Exception
	 */
	protected void execute(String sql, Object parameter) throws Exception
	{
		this.execute(sql, new Object[]{parameter});
	}
	
	/**
	 * Execute the SQL with the parameter.
	 * @param connection
	 * @param sql
	 * @param parameter
	 * @throws Exception
	 */
	protected void execute(Connection connection, String sql, Object parameter) throws Exception
	{
		this.execute(connection, sql, new Object[]{parameter});
	}	
	
	/**
	 * Execute the SQL with the parameters.
	 * @param sql
	 * @param parameters
	 * @throws Exception
	 */
	protected void execute(String sql, Object... parameters) throws Exception
	{
		//Fields
		Connection connection = null;
			
		//Try to execute the SQL
		try
		{
			//Get a connection
			connection = getConnection();
			
			//Execute the SQL using the connection
			execute(connection, sql, parameters);			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw ex;
		}
		
		//Clean up
		finally
		{
			try{connection.close();} catch(Throwable t){}
		}
		
	}
	
	/**
	 * Execute the SQL with the parameters on the connection.
	 * @param connection
	 * @param sql
	 * @param parameters
	 * @throws Exception
	 */
	protected void execute(Connection connection, String sql, Object... parameters) throws Exception
	{
		//Fields
		PreparedStatement statement = null;
		
		//Try to execute the SQL
		try
		{			
			//Create the statement
			statement = connection.prepareStatement(sql);
			
			//Set the parameters if set 
			if(parameters != null)
			{
				//Set the parameters
				for(int i=0; i<parameters.length; i++)
				{
					//Set the timestamp to avoid being shadowed by below date check
					if(parameters[i] instanceof Timestamp)
						statement.setObject(i+1, (Timestamp) parameters[i]);					
					
					//Convert to SQL date when using java.util.Date parameter
					else if(parameters[i] instanceof Date)
						statement.setObject(i+1, new java.sql.Date(((Date) parameters[i]).getTime()));

					//Set the parameter
					else statement.setObject(i+1, parameters[i]);
				}
			}
			
			//Execute the statement
			statement.execute();
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to execute the SQL", ex);
		}
		
		//Clean up
		finally
		{
			try {statement.close();} catch(Throwable t) {}
		}					
	}
	

}


//Namespace
package com.katujo.web.utils;


//Imports
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A database manager that has a cache of query results.
 * @author Johan Hertz
 */
public class DatabaseCacheManager extends DatabaseManager
{
	//The cache to hold the query results
	private final ConcurrentHashMap<String, CachedResult> cache = new ConcurrentHashMap<String, CachedResult>();
			
	//The maximum size of the cache
	private final int size;
	
	//The amount of results cleared from the cache when the size limit is reached 
	private final int clearSize;
	
	//The default timeout time for a cached result
	private final int timeout;
	
	//The flag if the cached is currently being cleaned
	private final AtomicBoolean cleaning = new AtomicBoolean(false);
			
	/**
	 * Create the object.
	 * @param defaultDataSource
	 * @param size
	 */
	public DatabaseCacheManager(String defaultDataSource, int size, int clearSize, int timeout)
	{
		//Call the super
		super(defaultDataSource);
		
		//Set the max cache size
		this.size = size;
		
		//Set the clear size
		this.clearSize = clearSize;
		
		//Set the timeout
		this.timeout = timeout;
	}
	
	/**
	 * Load a JSON object from the cache if not found then load object from 
	 * the database using the SQL and the parameters.
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public JsonObject getCacheObject(String sql, int timeout, Object...parameters) throws Exception
	{
		//Try to get the cache object
		try
		{
			//Create the key
			String key = createQueryKey(sql, parameters);
			
			//Get the cached result
			CachedResult cached = cache.get(key);
			
			//Get the current time
			long currentTime = System.currentTimeMillis();
			
			//Create a new cached result if current result is not found or old 
			if(cached == null || cached.getTimestamp() + timeout > currentTime)
				cache.put(key, cached = new CachedResult(this.getObject(sql, parameters)));
			
			//Update the hit time
			else cached.setHit(currentTime);
			
			//Check if to clean the cache
			if(this.cache.size() > this.size)
				cleanCache();
							
			//Return the cached result
			return cached.getResult().getAsJsonObject();
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to get cache object", ex);
		}		
	}
	
	/**
	 * Clean the cache.
	 */
	private void cleanCache()
	{
		//Don't do anything if already cleaning 
		if(cleaning.getAndSet(true))
			return;
		
		//Create a references to use in the thread 
		final ConcurrentHashMap<String, CachedResult> cache = this.cache;
		final int clearSize = this.clearSize;
		final AtomicBoolean cleaning = this.cleaning;
		
		//Create the thread to run
		Thread thread = new Thread()
		{									
			public void run()
			{
				//Create a map of results to remove from the cache
				//[String, CachedResult]
				List<Entry<String, CachedResult>> list = new ArrayList<Entry<String, CachedResult>>(cache.size());
				
				//Add the results
				list.addAll(cache.entrySet());
				
				//Create the comparator
				Comparator<Entry<String, CachedResult>> comparator = new Comparator<Entry<String, CachedResult>>()
				{
			        public int compare(Entry<String, CachedResult> result1, Entry<String, CachedResult> result2)
			        {
			                return (int) (result1.getValue().getHit()-result2.getValue().getHit());        
			        }						        
				};	
				
				//Sort the list
				Collections.sort(list, comparator);
				
				//Remove the number of result equal to the clear size or size of the list
				//which every is the smallest
				for(int i=0; i<list.size() && i<clearSize; i++)	
					cache.remove(list.get(i).getKey());
				
				//Set the cleaning flag
				cleaning.set(false);
			}			
		};
				
		//Start the cleaning thread
		thread.start();		
	}
	
	
	/*
	 * Utils
	 */
	
	/**
	 * Create the query key from the string.
	 * @param sql
	 * @param parameters
	 * @return
	 */
	private static String createQueryKey(String sql, Object... parameters)
	{
		//Create the string builder
		StringBuilder builder = new StringBuilder();
		
		//Add the SQL to the builder
		builder.append(sql);
		
		//Add the parameters to the builder
		for(Object obj : parameters)
			if(obj == null)
				builder.append("null");
			else builder.append(obj.toString());
		
		//Return the builder string
		return builder.toString();
	}	
	
	/**
	 * Holds a result for a query, used in the cache.
	 */
	private class CachedResult
	{
		//The result 
		private final JsonElement result;
		
		//The timestamp when the object where created
		private final long timestamp;
		
		//The timestamp for when the cached result were last hit
		private final AtomicLong hit;
		
		/**
		 * Create the object.
		 * @param result
		 */
		public CachedResult(JsonElement result)
		{
			//Set the result
			this.result = result;
			
			//Set the timestamp
			this.timestamp = System.currentTimeMillis();
			
			//Set the hit
			this.hit = new AtomicLong(this.timestamp);
		}

		/**
		 * Get the result.
		 * @return the result
		 */
		public JsonElement getResult()
		{
			return result;
		}

		/**
		 * Get the timestamp.
		 * @return the timestamp
		 */
		public long getTimestamp()
		{
			return timestamp;
		}
		
		/**
		 * Get the last time this cached result where hit.
		 * @return the hit
		 */
		public long getHit()
		{
			return hit.get();
		}
		
		/**
		 * Set the last time this cached result where hit.
		 * @param value
		 */
		public void setHit(long value)
		{
			hit.set(value);
		}		
	}
		
}

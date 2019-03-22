
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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * A database manager that has a cache of query results.
 * @author Johan Hertz
 */
public class DatabaseCacheManager extends com.katujo.web.utils.DatabaseManager
{
	//The cache to hold the query results
	private final ConcurrentHashMap<String, CachedResult> cache = new ConcurrentHashMap<String, CachedResult>();
	
	//The cache locks used when loading new data to the cache
	private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<String, Object>();
			
	//The maximum size of the cache
	private final int size;
	
	//The results size after the clean has run (size will be drained to this) 
	private final int clearSize;
	
	//The default expiry time for a cached result
	private final int expiry;
	
	//The flag if the cached is currently being cleaned
	private final AtomicBoolean cleaning = new AtomicBoolean(false);
	
	/**
	 * Create the object.
	 * <p>
	 * This sets up the cache with the following default values.<br>
	 * Cache size: 100<br>
	 * Clear size: 80<br>
	 * Expiry: 60 000 (one minute)
	 * </p>
	 * @param defaultDataSource
	 */
	public DatabaseCacheManager(String defaultDataSource)
	{
		//Call the super
		super(defaultDataSource);
		
		//Set the max cache size
		this.size = 100;
		
		//Set the clear size
		this.clearSize = 80;
		
		//Set the expiry
		this.expiry = 1 * 1000 * 60;		
	}
	
	/**
	 * Create the object.
	 * <p>
	 * This sets up the cache with the following default values.<br>
	 * Clear size: 80<br>
	 * Expiry: 60 000 (one minute)
	 * </p>
	 * @param defaultDataSource
	 * @param size
	 */
	public DatabaseCacheManager(String defaultDataSource, int size)
	{
		//Call the super
		super(defaultDataSource);
		
		//Set the max cache size
		this.size = size;
		
		//Set the clear size
		this.clearSize = 80;
		
		//Set the expiry
		this.expiry = 1 * 1000 * 60;		
	}		
	
	/**
	 * Create the object.
	 * <p>
	 * This sets up the cache with the following default values.<br>
	 * Expiry: 60 000 (one minute)
	 * </p>
	 * @param defaultDataSource
	 * @param size
	 * @param clearSize
	 * @param expiry
	 */
	public DatabaseCacheManager(String defaultDataSource, int size, int clearSize)
	{
		//Call the super
		super(defaultDataSource);
		
		//Set the max cache size
		this.size = size;
		
		//Set the clear size
		this.clearSize = clearSize;
		
		//Set the expiry
		this.expiry = 1 * 1000 * 60;
	}	
	
	/**
	 * Create the object.
	 * @param defaultDataSource
	 * @param size
	 * @param clearSize
	 * @param expiry
	 */
	public DatabaseCacheManager(String defaultDataSource, int size, int clearSize, int expiry)
	{
		//Call the super
		super(defaultDataSource);
		
		//Set the max cache size
		this.size = size;
		
		//Set the clear size
		this.clearSize = clearSize;
		
		//Set the expiry
		this.expiry = expiry;
	}
	
	/**
	 * Load a JSON array from the cache if not found or expired then load object from 
	 * the database using the SQL.
	 */
	public JsonArray getCacheArray(String sql) throws Exception
	{
		return getCacheArrayExpiry(sql, expiry, new Object[]{});
	}
	
	/**
	 * Load a JSON array from the cache if not found or expired then load object from 
	 * the database using the SQL and the parameters.
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public JsonArray getCacheArray(String sql, Object...parameters) throws Exception
	{
		return getCacheArrayExpiry(sql, expiry, parameters);
	}
	
	/**
	 * Load a JSON array from the cache if not found or expired then load object from 
	 * the database using the SQL and the parameters.
	 * @param sql
	 * @param expiry
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public JsonArray getCacheArrayExpiry(String sql, int expiry, Object...parameters) throws Exception
	{
		//Try to get the cache array
		try 
		{			
			//Create the key
			String key = createQueryKey(sql, parameters);
			
			//Get the lock
			Object lock = getLock(key);
						
			//Get the cached result
			CachedResult cached = cache.get(key);
			
			//Get the current time
			long currentTime = System.currentTimeMillis();
									
			//Create a new cached result if current result is not found or expired 
			if(cached == null || cached.getTimestamp() + expiry < currentTime)		
			{
				//Lock the call for a new result so only one call at a time can be made
				synchronized(lock)
				{					
					//Get the cached result again
					cached = cache.get(key);	
					
					//Check one more time to query for the new data 
					if(cached == null || cached.getTimestamp() + expiry < currentTime)										
						cache.put(key, cached = new CachedResult(this.getArray(sql, parameters)));
				}
			}
			
			//Update the hit time
			else cached.setHit(currentTime);
			
			//Check if to clean the cache
			if(this.cache.size() > this.size)
				cleanCache();
			
			//Get the cached result
			JsonElement element = cached.getResult();
			
			//Return null if not set
			if(element == null)
				return null;
							
			//Return the cached result
			return element.getAsJsonArray();				
		}
		
		//Failed
		catch(Exception ex) {throw new Exception("Failed to get cache array", ex);}				
	}	
	
	/**
	 * Load a JSON object from the cache if not found or expired then load object from
	 * the database using the SQL.
	 * <p>
	 * This method uses the <b>default</b> timeout.
	 * </p>
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public JsonObject getCacheObject(String sql) throws Exception
	{
		return getCacheObjectExpiry(sql, expiry, new Object[]{});
	}	
	
	/**
	 * Load a JSON object from the cache if not found or expired then load object from
	 * the database using the SQL and the parameters.
	 * <p>
	 * This method uses the <b>default</b> timeout.
	 * </p>
	 * @param sql
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public JsonObject getCacheObject(String sql, Object...parameters) throws Exception
	{
		return getCacheObjectExpiry(sql, expiry, parameters);
	}
	
	/**
	 * Load a JSON object from the cache if not found or expired then load object from 
	 * the database using the SQL and the parameters.
	 * @param sql
	 * @param expiry
	 * @param parameters
	 * @return
	 * @throws Exception
	 */
	public JsonObject getCacheObjectExpiry(String sql, int expiry, Object...parameters) throws Exception
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
												
			//Create a new cached result if current result is not found or expired 
			if(cached == null || cached.getTimestamp() + expiry < currentTime)				
				cache.put(key, cached = new CachedResult(this.getObject(sql, parameters)));
			
			//Update the hit time
			else cached.setHit(currentTime);
			
			//Check if to clean the cache
			if(cache.size() > size)
				cleanCache();
			
			//Get the cached result
			JsonElement element = cached.getResult();
			
			//Return null if not set
			if(element == null)
				return null;
							
			//Return the cached result
			return element.getAsJsonObject();				
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
		
		//Create references to use in the thread 
		final ConcurrentHashMap<String, CachedResult> cache = this.cache;
		final int clearSize = this.clearSize;
		final AtomicBoolean cleaning = this.cleaning;
		
		//Create the thread to run
		Thread thread = new Thread()
		{									
			public void run()
			{				
				//Create a list with all results in the cache
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
				
				//Sort the list so the oldest result is first in the list
				Collections.sort(list, comparator);
				
				//Remove results until list size = clearSize (or until list has no more results)
				for(int i=0; i<list.size() && cache.size()>clearSize; i++)						
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
	 * Get the lock used when refreshing the cache.
	 * @param queryKey
	 * @return
	 */
	private Object getLock(String queryKey)
	{
		//Get the lock 
		Object lock = locks.get(queryKey);
		
		//Create the lock if not set
		if(lock == null)
			this.locks.put(queryKey, lock = new Object());
		
		//Return the lock
		return lock;
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
			//Return null if result is not set
			if(result == null)
				return null;
			
			//The deep copy is needed since this object is shared 
			return result.deepCopy();
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

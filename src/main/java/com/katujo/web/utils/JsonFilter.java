
//Namespace
package com.katujo.web.utils;

//Java imports
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

//Google imports
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

/**
 * Parse the JSON data sent by the client and makes it available to other filters and servlets
 * via the thread safe getJson method.
 *  
 * @author Johan Hertz
 *
 */
public class JsonFilter implements Filter
{
	//The thread local field to hold the JSON data
	private static ThreadLocal<JsonElement> json = new ThreadLocal<>();
		
	/**
	 * Init the filter.
	 */
	@Override
	public void init(FilterConfig config) throws ServletException {}	
	
	/**
	 * Destroy the filter (clean up).
	 */
	@Override
	public void destroy() {}

	/**
	 * Run the filter.
	 */
	@Override
	public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException
	{		
		//Try to run the filter
		try
		{
			//Parse data from HTTP Request
			if(request instanceof HttpServletRequest)
			{				
				//Get the content type
				String contentType = request.getContentType();
				
				//Parse the JSON data from the request
				if(contentType != null && contentType.startsWith("application/json"))
				{
					//Get the charset
					String charset = contentType.split(";")[1].split("=")[1];
										
					//Get the input stream
					InputStream input = request.getInputStream();
					
					//Create the reader
					InputStreamReader reader = new InputStreamReader(input, charset);
					
					//Create the JSON and set it to the thread local field
					JsonFilter.json.set(new JsonParser().parse(reader));								
				}								
			}
															
			//Continue down the chain
			chain.doFilter(request, response);
		}
		
		//Failed
		catch(Exception ex) 
		{
			throw new ServletException(ex);
		}
		
		//Clear the JSON
		finally {JsonFilter.json.set(null);}
	}

	/**
	 * Get the JSON (for the current request thread).
	 * @return
	 */
	public static JsonElement getJson()
	{
		return json.get();
	}

}

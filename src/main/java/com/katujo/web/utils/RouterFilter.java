
//Namespace
package com.katujo.web.utils;

//Java imports
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

//Scannotation imports
import org.scannotation.AnnotationDB;
import org.scannotation.WarUrlFinder;

//Google imports
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * The router filter that routes incoming request to the annotated classes using the package and
 * the method name as the path.
 * @author Johan Hertz
 *
 */
public class RouterFilter implements Filter
{
	//The map to hold the routes <String=path, Route> 
	private final Map<String, Route> routes = new ConcurrentHashMap<String, Route>();
	
	//The no parameters object
	private static final Object[] NO_PARAMETERS = new Object[]{};	
	
	//The JSON content type
	private static final String JSON_CONTENT_TYPE = "application/json";
	
	//The character encoding to use when sending back JSON data
	private static final String JSON_CHARACTER_ENCODING = "ISO-8859-1";
	
	//The binary array content type
	private static final String BINARY_CONTENT_TYPE = "application/octet-stream"; 
	
	//The thread local field to hold the HTTP request data
	private static ThreadLocal<HttpServletRequest> request = new ThreadLocal<>();
	
	//The thread local field to hold the HTTP response data
	private static ThreadLocal<HttpServletResponse> response = new ThreadLocal<>();	
			
	/*
	 * Init the filter.
	 * (non-Javadoc)
	 * @see javax.servlet.Filter#init(javax.servlet.FilterConfig)
	 */
	@Override
	public void init(FilterConfig config) throws ServletException
	{
		//Try to init the filter
		try
		{			
			//Get the scanned classes
			Set<String> classesScanned = scanRoutes(config);
			
			//Get the web.xml classes
			Set<String> classesWebXml = webXmlRoutes(config);
			
			//Create the classes
			Set<String> classes = new HashSet<String>(classesScanned.size() + classesWebXml.size());
			
			//Add the classes
			classes.addAll(classesScanned);
			classes.addAll(classesWebXml);
			
			//Get the package to scan
			String scanPackage = config.getInitParameter("scan");			
						
			//Get the extension that will be added to the end of every path 
			String extension = config.getInitParameter("extension");
			
			//Get the print paths flag
			boolean printPaths = "true".equals(config.getInitParameter("print-paths"));
			
			//Create the route objects
			for(String clazz : classes)
			{
				//Get the routes class
				Class<?> routeClass = Class.forName(clazz);
				
				//Get the package name
				String routePackage = routeClass.getPackage().getName();
				
				//Check if the package match the scan package
				if(!routePackage.startsWith(scanPackage))
					continue;
																		
				//Create an instance of the route
				Object instance = routeClass.getConstructor(new Class[] {}).newInstance(new Object[]{});
				
				//Create the base path 
				String base = routePackage.substring(scanPackage.length()).replace('.', '/') + "/";
								
				//Add the controller methods
				for(Method method : routeClass.getMethods())
				{	
					//Only add public methods
					if(!"public".equals(Modifier.toString(method.getModifiers())))
						continue;
					
					//Only add methods that are declared in route class
					if(method.getDeclaringClass() != routeClass)
						continue;
					
					//Create the parameter type
					int type = Route.NO_PARAMETER;					
										
					//If the parameters length is 1 only allow JSON parameters and primitives
					if(method.getParameterTypes().length == 1)
					{						
						if(method.getParameterTypes()[0] == JsonObject.class) type = Route.JSON_OBJECT;
						else if(method.getParameterTypes()[0] == JsonArray.class) type = Route.JSON_ARRAY;
						else if(method.getParameterTypes()[0] == JsonElement.class) type = Route.JSON_ELEMENT;
						else if(method.getParameterTypes()[0] == boolean.class || method.getParameterTypes()[0] == Boolean.class) type = Route.PRIMITIVE_BOOLEAN;
						else if(method.getParameterTypes()[0] == double.class || method.getParameterTypes()[0] == Double.class) type = Route.PRIMITIVE_DOUBLE;
						else if(method.getParameterTypes()[0] == int.class || method.getParameterTypes()[0] == Integer.class) type = Route.PRIMITIVE_INT;
						else if(method.getParameterTypes()[0] == long.class || method.getParameterTypes()[0] == Long.class) type = Route.PRIMITIVE_LONG;
						else if(method.getParameterTypes()[0] == String.class) type = Route.PRIMITIVE_STRING;
						else continue;
					}	
					
					//If the parameter length is 2 only allow HttpServletRequest and HttpServletResponse
					else if(method.getParameterTypes().length == 2 &&
							method.getParameterTypes()[0] == HttpServletRequest.class && 
							method.getParameterTypes()[1] == HttpServletResponse.class)
							type = Route.REQUEST_RESPONSE;
					
					//Ignore any other method that has parameter
					else if(method.getParameterTypes().length != 0)
						continue;
					
					//Create the path
					String path = base + method.getName() + extension;

					//Check that the path has not already been added unique
					if(routes.containsKey(path))
						throw new Exception("Path " + path + " is not unique");
					
					//Add the path
					routes.put(path, new Route(instance, method, type));
				}	
			}
			
			//Create the out put list
			List<String> list = new ArrayList<>();
						
			//Print the paths setup
			for(String path : routes.keySet())
				list.add(routes.get(path).instance.getClass().getSimpleName() + "(" + (routes.get(path).method.getName() + "): " + path));
			
			//Sort the list
			Collections.sort(list);
			
			//Print the list
			for(String item : list)
				if(printPaths)
					System.out.println(item);
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new ServletException("Failed to init RouterFilter", ex);
		}				
	}
	
	/*
	 * Run the filter.
	 * (non-Javadoc)
	 * @see javax.servlet.Filter#doFilter(javax.servlet.ServletRequest, javax.servlet.ServletResponse, javax.servlet.FilterChain)
	 */
	@Override
	public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain chain) throws IOException, ServletException
	{
		//Try to run the filter
		try
		{					
			//Cast the request and response
			HttpServletRequest request = (HttpServletRequest) servletRequest;
			HttpServletResponse response = (HttpServletResponse) servletResponse;
			
			//Set the thread local fields
			RouterFilter.request.set(request);
			RouterFilter.response.set(response);
			
			//Invoke the route
			Object data = invoke(request, response);
			
			//Send the data as the response
			send(request, response, data);			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new ServletException("Failed to route request", ex);
		}
		
		//Clean up
		finally
		{
			RouterFilter.request.set(null);
			RouterFilter.response.set(null);
		}
	}
	
	/**
	 * Invoke a route.
	 * @param request
	 * @param response
	 * @return
	 * @throws Exception
	 */
	private Object invoke(HttpServletRequest request, HttpServletResponse response) throws Exception
	{
		//Create fields that will be used in the exception message
		String path = null;
		JsonElement requestData = null;
		
		//Try to invoke route
		try
		{
			//Get the path
			path = request.getServletPath();
			
			//Get the route
			Route route = routes.get(path);		
			
			//Check if there is a route for the path
			if(route == null)
				throw new Exception("Could not find a route for path " + path);
			
			//Create the return data
			Object returnData = null;
			
			//Get the request data
			requestData = JsonFilter.getJson();
			
			//Invoke the method on the instance with the no parameters
			if(route.parameterType == Route.NO_PARAMETER) 
				returnData = route.method.invoke(route.instance, NO_PARAMETERS);
			
			//Invoke the method on the instance with a JSON object parameter
			else if(route.parameterType == Route.JSON_OBJECT) 
				returnData = route.method.invoke(route.instance, requestData == null ? requestData : requestData.getAsJsonObject());
			
			//Invoke the method on the instance with a JSON array parameter
			else if(route.parameterType == Route.JSON_ARRAY) 
				returnData = route.method.invoke(route.instance, requestData == null ? requestData : requestData.getAsJsonArray());
			
			//Invoke the method on the instance with a JSON element parameter
			else if(route.parameterType == Route.JSON_ELEMENT) 
				returnData = route.method.invoke(route.instance, requestData);
			
			//Invoke the method on the instance with a boolean as the parameter
			else if(route.parameterType == Route.PRIMITIVE_BOOLEAN) 
				returnData = route.method.invoke(route.instance, requestData.getAsBoolean());
			
			//Invoke the method on the instance with a double as the parameter
			else if(route.parameterType == Route.PRIMITIVE_DOUBLE) 
				returnData = route.method.invoke(route.instance, requestData.getAsDouble());
			
			//Invoke the method on the instance with a integer as the parameter
			else if(route.parameterType == Route.PRIMITIVE_INT) 
				returnData = route.method.invoke(route.instance, requestData.getAsInt());
			
			//Invoke the method on the instance with a long as the parameter
			else if(route.parameterType == Route.PRIMITIVE_LONG) 
				returnData = route.method.invoke(route.instance, requestData.getAsLong());
			
			//Invoke the method on the instance with a string as the parameter
			else if(route.parameterType == Route.PRIMITIVE_STRING) 
				returnData = route.method.invoke(route.instance, requestData.getAsString());			
			
			//Invoke the method on the instance with the request and response parameters
			else if(route.parameterType == Route.REQUEST_RESPONSE) 
				returnData = route.method.invoke(route.instance, new Object[]{request, response});
			
			//The parameter type has not been implemented yet
			else throw new Exception("The parameter type " + route.parameterType + " has not been implemented");
			
			//Return the return data
			return returnData;
		}
		
		//Failed
		catch(Exception ex)
		{
			//Create the message
			String message = "Failed to invoke route for path " + path;
			
			//Check if request data is set
			if(requestData == null)
				message += " (request data not set)";
			
			//Add the request data to the exception
			else message += "\n\tRequest Data: " + requestData.toString();
			
			//Throw the exception
			throw new Exception(message, ex);
		}
	}
	
	/**
	 * Send the return data as the response.
	 * @param request
	 * @param response
	 * @param data
	 * @throws Exception
	 */
	private void send(HttpServletRequest request, HttpServletResponse response, Object data) throws Exception
	{
		//Try to send the data
		try
		{
			//Don't do anything if the response data is not set
			if(data == null)
				;
				
			//Send the response back as JSON data
			else if(data instanceof JsonElement)					
			{					
				//Set the response type
				response.setContentType(JSON_CONTENT_TYPE);
				response.setCharacterEncoding(JSON_CHARACTER_ENCODING);
																					
				//Print the JSON to the output stream
				response.getOutputStream().print(data.toString());					
			}
			
			//Send the response back as a JSON string primitive
			else if(data instanceof String)
			{
				//Set the response type
				response.setContentType(JSON_CONTENT_TYPE);
				response.setCharacterEncoding(JSON_CHARACTER_ENCODING);
				
				//Create the primitive
				JsonPrimitive primitive = new JsonPrimitive((String) data);
																					
				//Print the JSON to the output stream
				response.getOutputStream().print(primitive.toString());				
			}
						
			//Send the response back as a JSON number primitive
			else if(data instanceof Double || data instanceof Long || data instanceof Integer || data instanceof Number)
			{
				//Set the response type
				response.setContentType(JSON_CONTENT_TYPE);
				response.setCharacterEncoding(JSON_CHARACTER_ENCODING);
				
				//Create the primitive
				JsonPrimitive primitive = new JsonPrimitive((Number) data);
																					
				//Print the JSON to the output stream
				response.getOutputStream().print(primitive.toString());				
			}		
			
			//Send the response back as a JSON number primitive that can be cast as a date
			else if(data instanceof Date)
			{
				//Set the response type
				response.setContentType(JSON_CONTENT_TYPE);
				response.setCharacterEncoding(JSON_CHARACTER_ENCODING);
				
				//Create the primitive
				JsonPrimitive primitive = new JsonPrimitive(((Date) data).getTime());
																					
				//Print the JSON to the output stream
				response.getOutputStream().print(primitive.toString());					
			}
			
			//Send the response back as a JSON boolean primitive
			else if(data instanceof Boolean)
			{
				//Set the response type
				response.setContentType(JSON_CONTENT_TYPE);
				response.setCharacterEncoding(JSON_CHARACTER_ENCODING);
				
				//Create the primitive
				JsonPrimitive primitive = new JsonPrimitive((boolean) data);
																					
				//Print the JSON to the output stream
				response.getOutputStream().print(primitive.toString());					
			}			
			
			//Send the response back as binary data 
			else if(data instanceof byte[])
			{
				//Set the response type
				response.setContentType(BINARY_CONTENT_TYPE);
				
				//Write the data
				response.getOutputStream().write((byte[]) data);				
			}
			
			//The data type returned by the route is unrecognised
			else throw new Exception("Unrecognised route return type " + data.getClass().getCanonicalName());	
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to send data response", ex);
		}
	}

	/*
	 * Destroy the filter.
	 * (non-Javadoc)
	 * @see javax.servlet.Filter#destroy()
	 */
	@Override
	public void destroy() {}
	
	/**
	 * Get the request (for the current request thread).
	 * @return
	 */
	public static HttpServletRequest getRequest()
	{
		return request.get();
	}
	
	/**
	 * Get the response (for the current request thread).
	 * @return
	 */
	public static HttpServletResponse getResponse()
	{
		return response.get();
	}	
	
	/**
	 * Scan for the routes.
	 * @param config
	 * @return
	 * @throws Exception
	 */
	private static Set<String> scanRoutes(FilterConfig config) throws Exception
	{
		//Try to scan for routes
		try
		{			
			//Get the URL
			URL url = WarUrlFinder.findWebInfClassesPath(config.getServletContext());
			
			//Could not find the URL
			if(url == null)
				return new HashSet<String>();
			
			//Create the annotation database
			AnnotationDB database = new AnnotationDB();
			
			//Scan the URL
			database.scanArchives(url);
			
			//Get the classes marked with the controller annotation
			Set<String> classes = database.getAnnotationIndex().get(com.katujo.web.utils.Route.class.getCanonicalName());
			
			//Return the classes
			return classes;			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to scan for routes", ex);
		}
	}
	
	/**
	 * Read the routes from the web.xml file.
	 * @param config
	 * @return
	 * @throws Exception
	 */
	private static Set<String> webXmlRoutes(FilterConfig config) throws Exception
	{
		//Try to read the routes from the web.xml file
		try
		{
			//Get the routes
			String routes = config.getInitParameter("routes");
			
			//Check if routes is set
			if(routes == null)
				return new HashSet<String>();
			
			//Split the routes
			String[] split = routes.split(";");
			
			//Create the set
			Set<String> classes = new HashSet<String>();
			
			//Add the classes
			for(String item : split)
				if(!item.trim().equals(""))
					classes.add(item.trim());
			
			//Return the set
			return classes;			
		}
		
		//Failed
		catch(Exception ex)
		{
			throw new Exception("Failed to read web.xml routes", ex);
		}
	}

	/*
	 * Class to hold a route.
	 */
	private class Route
	{
		//Fields
		public Object instance;
		public Method method;
		public int parameterType;
		
		//Parameter types 
		public static final int NO_PARAMETER = 1;
		public static final int JSON_ELEMENT = 2;
		public static final int JSON_ARRAY = 3;
		public static final int JSON_OBJECT = 4;
		public static final int PRIMITIVE_BOOLEAN = 5;
		public static final int PRIMITIVE_DOUBLE = 6;
		public static final int PRIMITIVE_INT = 7;
		public static final int PRIMITIVE_LONG = 8;
		public static final int PRIMITIVE_STRING = 9;		
		public static final int REQUEST_RESPONSE = 10;
				
		/**
		 * Create the object.
		 * @param instance
		 * @param method
		 */
		public Route(Object instance, Method method, int parameterType)
		{
			this.instance = instance;
			this.method = method;
			this.parameterType = parameterType;
		}
	}
	
	
}

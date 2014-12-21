package edu.sjsu.cmpe.cache.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class CRDTClient
{
	
	
	private List<DistributedCacheService> list_server;
	private CountDownLatch countDown;
	final Map<String, Integer> map = new HashMap<String, Integer>();
	int flag = 0;
	int count=0;
	
	public ArrayList<CacheServiceInterface> cache_server;
	//public ArrayList<CacheServiceInterface> cache_server1 = new ArrayList<CacheServiceInterface>();
	
	final ArrayList<DistributedCacheService> writeServer = new ArrayList<DistributedCacheService>(3);

	
	public CRDTClient() {
		DistributedCacheService cache1 = new DistributedCacheService("http://localhost:3000");
		DistributedCacheService cache2 = new DistributedCacheService("http://localhost:3001");
		DistributedCacheService cache3 = new DistributedCacheService("http://localhost:3002");
		this.list_server = new ArrayList<DistributedCacheService>();

		list_server.add(cache1);
		list_server.add(cache2);
		list_server.add(cache3);
	}


	public boolean writeKey(long key, String value) throws Exception {
		this.countDown = new CountDownLatch(list_server.size());
		for (final DistributedCacheService distributed_cache : list_server) {
			Future<HttpResponse<JsonNode>> future = Unirest.put(distributed_cache.getServerUrl()+ "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value)
					.asJsonAsync(new Callback<JsonNode>() {
						
						@Override
						public void failed(UnirestException e) {
							//System.out.println("Request Failed "+cacheServer.getServerUrl());
							countDown.countDown();
						}
						@Override
						public void completed(HttpResponse<JsonNode> response) {
							
							count++;
							writeServer.add(distributed_cache);
							//System.out.println("Successful "+cacheServer.getServerUrl());
							countDown.countDown();
						}
						
						@Override
						public void cancelled() {
							countDown.countDown();
						}

					});
		}
		this.countDown.await();
		if (count > 1) {
			System.out.println("Key-value successfully written.");
			return true;
		}
		else //Delete call to clean up partial state.
		{
			
			System.out.println("--- Deleting Current Value ---");
			
			this.countDown = new CountDownLatch(writeServer.size());
			for (DistributedCacheService distributed_cache : writeServer) {
				Future<HttpResponse<JsonNode>> future = Unirest.get(distributed_cache.getServerUrl() + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {
							
							@Override
							public void failed(UnirestException e) {
								//System.out.println("Failed. In Delete"+cacheServer.getServerUrl());
								countDown.countDown();
							}
							
							@Override
							public void completed(HttpResponse<JsonNode> response) {
								//System.out.println("Successful. In delete "+cacheServer.getServerUrl());
								countDown.countDown();
							}

							@Override
							public void cancelled() {
								//System.out.println("Cancelled. In delete");
								countDown.countDown();
							}
					});
			}
			this.countDown.await(2, TimeUnit.SECONDS);
			Unirest.shutdown();
			return false;
			
		}
		
	}
	

	
	
	
	public String getValue(Map<String, Integer> valueMap, int value) {
		for (Entry<String, Integer> entry : valueMap.entrySet()) {
			if (value == entry.getValue()) 
			{
				return entry.getKey();
			}
		}
		return null;
	}
	
	
	public String get(long key) throws Exception {
		this.countDown = new CountDownLatch(list_server.size());
		final Map<DistributedCacheService, String> map1 = new HashMap<DistributedCacheService, String>();
		for (final DistributedCacheService distributed_cache : list_server) {
			Future<HttpResponse<JsonNode>> future = Unirest.get(distributed_cache.getServerUrl() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {

						@Override
						public void failed(UnirestException e) {
							System.out.println("Failed. In get");
							countDown.countDown();
						}

						@Override
						public void completed(HttpResponse<JsonNode> response) {
							map1.put(distributed_cache, response.getBody().getObject().getString("value"));
							//System.out.println("The request is successful "+distributed_cache.getServerUrl());
							countDown.countDown();
						}

						@Override
						public void cancelled() {
							System.out.println("Cancelled");
							countDown.countDown();
						}
				});
		}
		this.countDown.await(2, TimeUnit.SECONDS);
		
		for (String value : map1.values()) {
			int count = 1;
			if (map.containsKey(value)) {
				count = map.get(value);
				count++;
			}
			if (flag<count)
				flag=count;
			map.put(value, count);
		}
	
		
		String value = this.getValue(map,flag);
		
		
	//Read on Repair
		if (flag!= this.list_server.size()) {
			for (Entry<DistributedCacheService, String> cacheServer_response : map1.entrySet()) {
				if (!value.equals(cacheServer_response.getValue())) {
					
					HttpResponse<JsonNode> response = Unirest.put(cacheServer_response.getKey() + "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(key))
							.routeParam("value", value)
							.asJson();
				}
			}
			
			for (DistributedCacheService distributed_cache : this.list_server) {
				if (map1.containsKey(distributed_cache)) continue;
				
				HttpResponse<JsonNode> response = Unirest.put(distributed_cache.getServerUrl() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value)
						.asJson();
			}
		} 
		
		return value;
	}
	
	
}

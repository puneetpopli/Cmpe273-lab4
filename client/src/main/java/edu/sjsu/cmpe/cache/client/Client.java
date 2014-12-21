package edu.sjsu.cmpe.cache.client;

	import java.util.ArrayList;
	import java.util.List;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        String str;
        
        
        
        
        CRDTClient crdtClient = new CRDTClient();
        boolean temp = crdtClient.writeKey(1, "a");
        if (temp) 
        {
        	
        	System.out.println("--------- Write 1=>a ------");
        	System.out.println("Sleeping for 20 seconds....");
        	Thread.sleep(20000);
        	temp = crdtClient.writeKey(1, "b");
        	
        	if (temp)
        	{
        		System.out.println("---------- Write 1=>b --------");
        		System.out.println("Sleeping for 20 seconds....");
            	Thread.sleep(20000);
            	str = crdtClient.get(1);
            	System.out.println("Value = " +str);
        	} 
        	else 
        	{
            	System.out.println("Failed. In first if");
        	}
        } 
        else 
        {
        	
        	System.out.println("Failed");
        }	
        
    }

}

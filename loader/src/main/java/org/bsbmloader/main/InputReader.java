package org.bsbmloader.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

public class InputReader {
	
    public String readInput(String message){
    	String input = "";
    	try{
    		InputStreamReader reader = new InputStreamReader(System.in);
    		BufferedReader readbuffer = new BufferedReader(reader);
    		System.out.print(message);
    	    input = readbuffer.readLine();
    	}catch(IOException e){
    		System.out.println(e.getMessage());
    	}
    	
    	return input.trim();
    }
	
    //For testing
    public String readInput(Scanner scanner){
    	String input = "";
        input = scanner.next();	
    	return input.trim();
    }

}

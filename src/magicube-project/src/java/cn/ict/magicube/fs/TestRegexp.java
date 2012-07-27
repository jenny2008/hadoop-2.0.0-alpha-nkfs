package cn.ict.magicube.fs;

import java.io.BufferedInputStream;
import java.io.Console; 
import java.io.IOException;
import java.io.StringReader;
import java.util.regex.Pattern; 
import java.util.regex.Matcher; 
 
public class TestRegexp { 
 
    public static void main(String[] args) throws IOException {
    	
    	StringReader sr = new StringReader("1234567");
    	char[] cbuf = new char[10];
    	cbuf[0] = 'x';
    	sr.read(cbuf, 1, 3);
    	
    	StringBuilder b = new StringBuilder("parities");
    	b.append(" ");
    	b.append(10);
    	b.append(" ");
    	b.append(20);
    	System.out.println(b.toString());
    	
        Console console = System.console(); 
        if (console == null) { 
            System.err.println("No console."); 
            System.exit(1); 
        } 
       
        while (true) { 
            Pattern pattern = Pattern.compile(console.readLine("%nEnter your regex: ")); 
            Matcher matcher = pattern.matcher(console.readLine("Enter input string to search: ")); 
            boolean found = false;
            
            console.format("Group number %d\n", matcher.groupCount());
            while (matcher.find()) { 
                console.format("I found the text \"%s\" starting at index %d " + 
                        "and ending at index %d.%n", 
                        matcher.group(), matcher.start(), matcher.end());
                
                for (int i = 0; i < matcher.groupCount(); i++) {
                	console.format("group %d: %s\n",
                			i + 1, matcher.group(i + 1));
                }
                
                found = true;
            } 
            if (!found) { 
                console.format("No match found.%n"); 
            } 
        } 
    } 
}

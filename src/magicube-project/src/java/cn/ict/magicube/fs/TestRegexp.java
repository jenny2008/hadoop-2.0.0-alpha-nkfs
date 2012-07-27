package cn.ict.magicube.fs;

import java.io.Console; 
import java.util.regex.Pattern; 
import java.util.regex.Matcher; 
 
public class TestRegexp { 
 
    public static void main(String[] args) { 
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

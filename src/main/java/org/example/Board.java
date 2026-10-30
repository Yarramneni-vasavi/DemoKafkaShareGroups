package org.example;

import java.time.*; import java.time.format.DateTimeFormatter; import java.util.*;

public class Board {
    private final EventBus bus = new EventBus();
    public void show(String topic) { List<String> events=bus.readAll("board-"+UUID.randomUUID()); Map<String,String> state=new LinkedHashMap<>(); List<String> recent=new ArrayList<>(); for(String e:events){String key=key(e); String ev=field(e,"event"); state.put(key, ev); recent.add(ev+" "+key+" delivery "+field(e,"deliveryCount"));} int a=0,b=0,c=0,d=0; for(String s:state.values())if(s.equals("PRODUCED")||s.equals("RELEASED"))a++;else if(s.equals("ACQUIRED"))b++;else if(s.equals("ACCEPTED"))c++;else if(s.equals("ARCHIVED"))d++; System.out.println("\n"+topic+" · Available  A Acquired  ✓ Acknowledged  ✕ Archived"); System.out.println("Available "+a+"   Acquired "+b+"   Acknowledged "+c+"   Archived "+d); System.out.println("\nRecent events"); recent.stream().skip(Math.max(0,recent.size()-8)).forEach(x->System.out.println("  "+x)); }
    public void live(String topic){ System.out.println("Press Enter to return to the menu."); try { while (true) { show(topic); Thread.sleep(250); if (System.in.available() > 0) { new java.io.BufferedReader(new java.io.InputStreamReader(System.in)).readLine(); return; } } } catch (Exception e) { Thread.currentThread().interrupt(); } }
    private static String key(String j){return field(j,"partition")+"@"+field(j,"offset");} private static String field(String j,String f){String n="\""+f+"\":";int i=j.indexOf(n);if(i<0)return "?";i+=n.length();if(j.charAt(i)=='\"'){int e=j.indexOf('"',i+1);return j.substring(i+1,e);}int e=i;while(e<j.length()&&",}".indexOf(j.charAt(e))<0)e++;return j.substring(i,e);}
}

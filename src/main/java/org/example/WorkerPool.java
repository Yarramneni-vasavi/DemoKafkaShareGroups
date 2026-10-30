package org.example;

import java.util.*;
import java.util.concurrent.*;

public class WorkerPool implements AutoCloseable {
    private final List<Thread> threads = new ArrayList<>(); private final List<Object> workers = new ArrayList<>(); private final EventBus events = new EventBus(); private final String topic; private boolean classic;
    public WorkerPool(String topic, int count, boolean classic) { this.topic=topic; this.classic=classic; for(int i=1;i<=count;i++){ String name="w"+i; Object w=classic?new ConsumerGroupWorker(topic,name,events):new ShareWorker(topic,name,events); workers.add(w); Thread t=new Thread((Runnable)w,name); threads.add(t); t.start(); } }
    public void kill(int n) { if(n>=1&&n<=workers.size()&&workers.get(n-1) instanceof ShareWorker s) { s.kill(); threads.get(n-1).interrupt(); } }
    public void stopAll() { for(Object w:workers) if(w instanceof ShareWorker s)s.stop(); else if(w instanceof ConsumerGroupWorker c)c.stop(); for(Thread t:threads) try{t.join(3000);}catch(InterruptedException e){Thread.currentThread().interrupt();} events.close(); }
    public int size(){return threads.size();}
    @Override public void close(){stopAll();}
}

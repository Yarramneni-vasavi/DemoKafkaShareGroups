package org.example;

import java.util.*;

public class Main {
    private final Scanner in = new Scanner(System.in); private final TopicService topics = new TopicService(); private WorkerPool pool;
    public static void main(String[] args) { new Main().run(); }
    private void run() { System.out.println("Kafka Share Groups Demo"); while(true){ System.out.println("\n1 Create topic\n2 Produce records\n3 Start workers\n4 Board\n5 Group internals\n6 Inspect storage\n7 Kill worker\n8 Reset\n0 Exit"); String choice=ask("Choose"); try { switch(choice){case "1"->create();case "2"->produce();case "3"->workers();case "4"->new Board().live(Config.TOPIC);case "5"->internals();case "6"->storage();case "7"->kill();case "8"->reset();case "0"->{if(pool!=null)pool.stopAll();return;}default->System.out.println("Choose a number from the menu.");} }catch(Exception e){System.out.println("Operation failed: "+e.getMessage());} } }
    private String ask(String label){System.out.print(label+": ");return in.nextLine().trim();} private String ask(String label,String def){String s=ask(label+" ["+def+"]");return s.isBlank()?def:s;}
    private void create(){String name=ask("Topic",Config.TOPIC);int p=Integer.parseInt(ask("Partitions","3"));topics.ensure(name,p);topics.ensureEvents();configureShareGroup();ShellRunner.run(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-topics.sh","--bootstrap-server","localhost:9092","--describe","--topic",name));}
    private void produce(){ensureNoConsoleShareConsumer();String name=ask("Topic",Config.TOPIC);requireTopic(name);int n=Integer.parseInt(ask("Count","100"));Set<Integer> poison=new HashSet<>();String raw=ask("Poison offsets (comma separated, blank for none)");if(!raw.isBlank())for(String x:raw.split(","))poison.add(Integer.parseInt(x.trim()));new ProducerService().produce(name,n,poison);}
    private void workers(){ensureNoConsoleShareConsumer();requireTopic(Config.TOPIC);configureShareGroup();if(pool!=null)pool.stopAll();int n=Integer.parseInt(ask("Worker count","6"));pool=new WorkerPool(Config.TOPIC,n,false);System.out.println("Started "+n+" share workers.");}
    private void internals(){
        for(String detail:List.of("--offsets","--members","--state")) ShellRunner.run(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-share-groups.sh","--describe","--bootstrap-server","localhost:9092","--group",Config.SHARE_GROUP,detail));
    }
    private void storage(){
        String command = "base=/tmp/kafka-logs; [ -d \"$base\" ] || base=/var/lib/kafka/data; "
                + "f=$(find \"$base\" -type f -path '*/" + Config.TOPIC + "-0/*.log' | sort | head -1); "
                + "if [ -z \"$f\" ]; then echo 'No log segment found for topic " + Config.TOPIC + " partition 0'; exit 1; fi; "
                + "echo \"Dumping $f\"; /opt/kafka/bin/kafka-dump-log.sh --files \"$f\" --print-data-log";
        ShellRunner.run(List.of("docker","exec","kafka","sh","-c",command));
    }
    private void kill(){if(pool==null){System.out.println("No workers running.");return;}pool.kill(Integer.parseInt(ask("Worker number","1")));}
    private void reset(){
        if(pool!=null){pool.stopAll();pool=null;}
        // Share-group state must be removed as well as the topic. Otherwise a
        // recreated topic can start below the group's old start offset.
        ShellRunner.Result deletion = ShellRunner.runResult(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-share-groups.sh","--delete","--bootstrap-server","localhost:9092","--group",Config.SHARE_GROUP));
        String deleteOutput = deletion.output().toLowerCase(Locale.ROOT);
        boolean groupMissing = deleteOutput.contains("not exist") || deleteOutput.contains("not found");
        if(deletion.exitCode()!=0 && !groupMissing)
            throw new IllegalStateException("Reset stopped: demo-share still has an active member. Stop external/console consumers and retry.");
        topics.delete(Config.TOPIC); topics.delete(Config.EVENTS_TOPIC); topics.ensureEvents();
        System.out.println("Reset complete.");
    }
    private void ensureNoConsoleShareConsumer(){
        ShellRunner.Result groups = ShellRunner.runResult(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-share-groups.sh","--list","--bootstrap-server","localhost:9092"));
        if(groups.exitCode()!=0 || !groups.output().lines().anyMatch(line -> line.trim().equals(Config.SHARE_GROUP))) return;
        ShellRunner.Result members = ShellRunner.runResult(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-share-groups.sh","--describe","--bootstrap-server","localhost:9092","--group",Config.SHARE_GROUP,"--members"));
        if(members.output().contains("console-share-consumer"))
            throw new IllegalStateException("A console-share-consumer is still in demo-share. Stop it before producing or starting workers.");
    }
    private void requireTopic(String name){
        if(!topics.exists(name)) throw new IllegalStateException("Topic " + name + " does not exist. Choose option 1 before Produce or Start workers.");
    }
    private void configureShareGroup(){
        ShellRunner.Result result = ShellRunner.runResult(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-configs.sh","--bootstrap-server","localhost:9092","--alter","--entity-type","groups","--entity-name",Config.SHARE_GROUP,"--add-config","share.auto.offset.reset=earliest,share.record.lock.duration.ms=5000"));
        if(result.exitCode()!=0) throw new IllegalStateException("Unable to configure " + Config.SHARE_GROUP);
    }
}

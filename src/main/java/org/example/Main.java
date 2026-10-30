package org.example;

import java.util.*;

public class Main {
    private final Scanner in = new Scanner(System.in); private final TopicService topics = new TopicService(); private WorkerPool pool;
    public static void main(String[] args) { new Main().run(); }
    private void run() { System.out.println("Kafka Share Groups Demo"); while(true){ System.out.println("\n1 Create topic\n2 Produce records\n3 Start workers\n4 Board\n5 Group internals\n6 Inspect storage\n7 Kill worker\n8 Reset\n0 Exit"); String choice=ask("Choose"); try { switch(choice){case "1"->create();case "2"->produce();case "3"->workers();case "4"->new Board().live(Config.TOPIC);case "5"->internals();case "6"->storage();case "7"->kill();case "8"->reset();case "0"->{if(pool!=null)pool.stopAll();return;}default->System.out.println("Choose a number from the menu.");} }catch(Exception e){System.out.println("Operation failed: "+e.getMessage());} } }
    private String ask(String label){System.out.print(label+": ");return in.nextLine().trim();} private String ask(String label,String def){String s=ask(label+" ["+def+"]");return s.isBlank()?def:s;}
    private void create(){String name=ask("Topic",Config.TOPIC);int p=Integer.parseInt(ask("Partitions","3"));topics.ensure(name,p);topics.ensureEvents();ShellRunner.run(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-configs.sh","--bootstrap-server","localhost:9092","--alter","--entity-type","groups","--entity-name",Config.SHARE_GROUP,"--add-config","share.auto.offset.reset=earliest,share.record.lock.duration.ms=5000"));ShellRunner.run(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-topics.sh","--bootstrap-server","localhost:9092","--describe","--topic",name));}
    private void produce(){String name=ask("Topic",Config.TOPIC);int n=Integer.parseInt(ask("Count","100"));Set<Integer> poison=new HashSet<>();String raw=ask("Poison offsets (comma separated, blank for none)");if(!raw.isBlank())for(String x:raw.split(","))poison.add(Integer.parseInt(x.trim()));new ProducerService().produce(name,n,poison);}
    private void workers(){if(pool!=null)pool.stopAll();int n=Integer.parseInt(ask("Worker count","6"));pool=new WorkerPool(Config.TOPIC,n,false);System.out.println("Started "+n+" share workers.");}
    private void internals(){
        for(String detail:List.of("--offsets","--members","--state")) ShellRunner.run(List.of("docker","exec","kafka","/opt/kafka/bin/kafka-share-groups.sh","--describe","--bootstrap-server","localhost:9092","--group",Config.SHARE_GROUP,detail));
        System.out.println("\nStarting the Kafka console share consumer for group " + Config.SHARE_GROUP + ". Press Ctrl+C to return to this menu.");
        ShellRunner.runInteractive(List.of("docker","exec","-i","kafka","/opt/kafka/bin/kafka-console-share-consumer.sh","--bootstrap-server","localhost:9092","--topic",Config.TOPIC,"--group",Config.SHARE_GROUP));
    }
    private void storage(){
        String command = "base=/tmp/kafka-logs; [ -d \"$base\" ] || base=/var/lib/kafka/data; "
                + "f=$(find \"$base\" -type f -path '*/" + Config.TOPIC + "-0/*.log' | sort | head -1); "
                + "if [ -z \"$f\" ]; then echo 'No log segment found for topic " + Config.TOPIC + " partition 0'; exit 1; fi; "
                + "echo \"Dumping $f\"; /opt/kafka/bin/kafka-dump-log.sh --files \"$f\" --print-data-log";
        ShellRunner.run(List.of("docker","exec","kafka","sh","-c",command));
    }
    private void kill(){if(pool==null){System.out.println("No workers running.");return;}pool.kill(Integer.parseInt(ask("Worker number","1")));}
    private void reset(){if(pool!=null){pool.stopAll();pool=null;}topics.delete(Config.TOPIC);topics.delete(Config.EVENTS_TOPIC);topics.ensureEvents();System.out.println("Reset complete.");}
}

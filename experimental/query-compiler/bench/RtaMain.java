import org.questdb.experimental.codegen.RtaCompilerSetup;
import io.questdb.*;
import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.griffin.*;
import io.questdb.log.LogFactory;
import io.questdb.mp.*;
import io.questdb.std.*;
import io.questdb.std.str.*;
import java.nio.file.Path;
import java.nio.file.Files;
import java.lang.management.ManagementFactory;
import java.util.*;

public class RtaMain {
 static int workers;
 public static void main(String[] a) throws Exception {
  System.out.println("PID="+ProcessHandle.current().pid());
  String mode=a[0];int cores=Integer.parseInt(a[1]);workers=cores;Path root=Path.of(a[2]),source=Path.of(a[3]),out=Path.of(a[4]);Files.createDirectories(out);
  var p=new Properties();p.setProperty("http.enabled","false");p.setProperty("shared.query.worker.count",Integer.toString(cores));p.setProperty("cairo.query.memory.limit.bytes","40G");
  var config=new PropServerConfiguration(root.toString(),p,Map.of(),LogFactory.getLog(RtaMain.class),new BuildInformationHolder());
  var cfg=config.getCairoConfiguration();Files.createDirectories(Path.of(cfg.getDbRoot().toString()));
  try(var free=new FreeOnExit();var engine=new CairoEngine(cfg)) {
   config.init(engine,free);
   try(var pool=new WorkerPool(config.getSharedWorkerPoolQueryConfiguration());var ctx=new SqlExecutionContextImpl(engine,cores).with(AllowAllSecurityContext.INSTANCE,null,null,-1,null)) {
    WorkerPoolUtils.setupQueryJobs(pool,engine);pool.start();
    System.out.printf("CONFIG workers=%d jit=%d frame=%d:%d shard=%d mode=%s%n",cores,cfg.getSqlJitMode(),cfg.getSqlPageFrameMinRows(),cfg.getSqlPageFrameMaxRows(),cfg.getGroupByShardingThreshold(),config.getSharedWorkerPoolQueryConfiguration().getWorkerPoolMode());
    if(mode.equals("load")) {
     for(String ddl:Files.readString(source).split(";"))if(!ddl.isBlank())engine.execute(ddl,ctx);
     System.out.println("LOADED");return;
    }
    if(mode.equals("llvm")){System.setProperty("rta.llvm","true");RtaCompilerSetup.install(ctx,out);}
    var queries=Files.isDirectory(source)?Files.list(source).filter(x->x.toString().endsWith(".sql")).sorted().toList():List.of(source);
    for(Path query:queries) {
     String name=query.getFileName().toString().replace(".sql","");Path dest=out.resolve(name);Files.createDirectories(dest);
     try { Files.deleteIfExists(dest.resolve("error.txt")); run(engine,ctx,query,dest); }
     catch(Throwable e) {Files.writeString(dest.resolve("error.txt"),e.toString());System.out.println("ERROR query="+name+" "+e);e.printStackTrace(System.out);}
    }
   }
  } finally {LogFactory.haltInstance();}
 }
 static void run(CairoEngine engine,SqlExecutionContextImpl ctx,Path query,Path dest)throws Exception {
  String sql=Files.readString(query).strip();
  if(System.getenv().getOrDefault("RTA_DROP_CACHE","0").equals("1")) {
   if(new ProcessBuilder("sync").start().waitFor()!=0)throw new IllegalStateException("sync");
   var drop=new ProcessBuilder("sudo","-n","tee","/proc/sys/vm/drop_caches").redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
   drop.getOutputStream().write("3\n".getBytes());drop.getOutputStream().close();if(drop.waitFor()!=0)throw new IllegalStateException("drop caches");
  }
  var os=(com.sun.management.OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
  long compileCpu0=os.getProcessCpuTime(),compileReads0=reads(),compileFaults0=faults(),begin=System.nanoTime();
  try(var compiler=engine.getSqlCompiler();var factory=compiler.compile(sql,ctx).getRecordCursorFactory()) {
   if(factory==null)throw new IllegalStateException("No SELECT factory");
   long compile=System.nanoTime()-begin,compileCpu=os.getProcessCpuTime()-compileCpu0,compileReads=reads()-compileReads0,compileFaults=faults()-compileFaults0;var meta=factory.getMetadata();var plan=new TextPlanSink();plan.of(factory,ctx);
   var lines=new ArrayList<String>();for(int i=1;i<=plan.getLineCount();i++)lines.add(plan.getLine(i).toString());Files.write(dest.resolve("plan.txt"),lines);
   System.out.println("FACTORY="+factory.getClass().getName());
   var types=new ArrayList<String>();for(int i=0;i<meta.getColumnCount();i++)types.add(meta.getColumnName(i)+"\t"+ColumnType.nameOf(meta.getColumnType(i)));Files.write(dest.resolve("types.tsv"),types);
   System.out.printf("COMPILE query=%s ns=%d cpu_ns=%d read_bytes=%d major_faults=%d%n",query.getFileName(),compile,compileCpu,compileReads,compileFaults);
   var counters=new ArrayList<java.util.function.LongSupplier>();collectCounters(factory,counters, new IdentityHashMap<>());
   try(var profile=new PerfControl()) {
    for(int run=0;run<Integer.parseInt(System.getenv().getOrDefault("RTA_REPS","1"));run++) {
     long bytes=reads(),faults=faults(),nativeBefore=counters.stream().mapToLong(java.util.function.LongSupplier::getAsLong).sum();
     int warmups=Integer.parseInt(System.getenv().getOrDefault("RTA_WARMUPS","5"));
     if(run>=warmups)profile.command("enable");long cpu0=os.getProcessCpuTime(),start=System.nanoTime();var rows=new ArrayList<String>();
     try(var cursor=factory.getCursor(ctx)) {var rec=cursor.getRecord();while(cursor.hasNext()) {var row=new StringSink();row.put('[');for(int c=0;c<meta.getColumnCount();c++){if(c>0)row.put(',');row.put('"').escapeJsonStr(value(rec,meta,c)).put('"');}row.put(']');rows.add(row.toString());}}
     long ns=System.nanoTime()-start,cpu=os.getProcessCpuTime()-cpu0;if(run>=warmups)profile.command("disable");
     long nativeRows=counters.stream().mapToLong(java.util.function.LongSupplier::getAsLong).sum()-nativeBefore;
     System.out.printf("RESULT query=%s run=%d ns=%d cpu_ns=%d rows=%d read_bytes=%d major_faults=%d native_rows=%d%n",query.getFileName(),run,ns,cpu,rows.size(),reads()-bytes,faults()-faults,nativeRows);
     if(modeIsLlvm())RtaCompilerSetup.report(run);
     Files.write(dest.resolve("result-"+run+".jsonl"),rows);System.out.flush();
    }
   }
  }
 }
 static boolean modeIsLlvm() { return Boolean.getBoolean("rta.llvm"); }
 static void collectCounters(RecordCursorFactory factory,List<java.util.function.LongSupplier> counters,IdentityHashMap<Object,Boolean> seen)throws Exception {
  if(factory==null || seen.put(factory,Boolean.TRUE)!=null)return;
  try {
   Object compiled=factory.getClass().getMethod("getCompiledAggregate").invoke(factory);
   if(compiled instanceof io.questdb.griffin.engine.groupby.vect.CompiledVectorAggregate vector)counters.add(vector::getNativeRows);
  }catch(NoSuchMethodException ignored){}
  try {
   Object atom=factory.getClass().getMethod("getAtom").invoke(factory);
   if(atom!=null) {
   for(String name:List.of("getCompiledAggregateNativeRows","getCompiledJoinNativeRows")) {
    try {var m=atom.getClass().getMethod(name);counters.add(()->{try{return (long)m.invoke(atom);}catch(Exception e){throw new RuntimeException(e);}});}catch(NoSuchMethodException ignored){}
   }
   try {
    var getter=atom.getClass().getMethod("getCompiledUpdater",int.class);
    var m=Class.forName("io.questdb.griffin.engine.groupby.GroupByKernel$Updater").getMethod("getNativeMapRows");
    for(int slot=-1;slot<workers;slot++) {
     int id=slot;counters.add(()->{try{Object updater=getter.invoke(atom,id);return updater!=null&&m.getDeclaringClass().isInstance(updater)?(long)m.invoke(updater):0;}catch(Exception e){throw new RuntimeException(e);}});
    }
   }catch(NoSuchMethodException|ClassNotFoundException ignored){}
   }
  }catch(NoSuchMethodException ignored){}
  collectCounters(factory.getBaseFactory(),counters,seen);
 }
 static long reads()throws Exception {for(String line:Files.readAllLines(Path.of("/proc/self/io")))if(line.startsWith("read_bytes:"))return Long.parseLong(line.substring(11).strip());return 0;}
 static long faults()throws Exception {String s=Files.readString(Path.of("/proc/self/stat"));return Long.parseLong(s.substring(s.lastIndexOf(')')+2).split(" ")[9]);}
 static String value(io.questdb.cairo.sql.Record r,RecordMetadata m,int c) {
  return switch(ColumnType.tagOf(m.getColumnType(c))) {
   case ColumnType.DOUBLE -> {double v=r.getDouble(c);yield Double.isNaN(v)?"\\N":Double.toString(v);}
   case ColumnType.FLOAT -> {float v=r.getFloat(c);yield Float.isNaN(v)?"\\N":Float.toString(v);}
   case ColumnType.INT -> {int v=r.getInt(c);yield v==Numbers.INT_NULL?"\\N":Integer.toString(v);}
   case ColumnType.LONG -> {long v=r.getLong(c);yield v==Numbers.LONG_NULL?"\\N":Long.toString(v);}
   case ColumnType.DATE -> Long.toString(r.getDate(c));
   case ColumnType.TIMESTAMP -> Long.toString(r.getTimestamp(c));
   default -> {var s=new StringSink();CursorPrinter.printColumn(r,m,c,s,false,false,"\\N");yield s.toString();}
  };
 }
 static class PerfControl implements AutoCloseable {
  java.io.BufferedWriter writer;java.io.BufferedReader reader;
  PerfControl()throws Exception {String ctl=System.getenv("QUERY_CODEGEN_PERF_CONTROL");if(ctl!=null){writer=Files.newBufferedWriter(Path.of(ctl));reader=Files.newBufferedReader(Path.of(System.getenv("QUERY_CODEGEN_PERF_ACK")));}}
  void command(String command)throws Exception {if(writer!=null){writer.write(command+"\n");writer.flush();if(!reader.readLine().replace("\0","").strip().equals("ack"))throw new IllegalStateException("perf acknowledgement");}}
  public void close()throws Exception {if(writer!=null){writer.close();reader.close();}}
 }
}

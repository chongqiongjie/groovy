#! /bin/groovy -Dgroovy.grape.report.downloads=true -Divy.message.logger.level=1
/**********************************
 * [intro]
 *   author=larluo@spiderdt.com
 *   func=merge algorithm for data warehouse
 *=================================
 * [param]
 *   job_id=
 *=================================
 * [caller]
 *   spark_etl_merge.groovy d_bolome_product_category '[[":barcode", 0]]'
 *=================================
 * [version]
 *   v1_0=2016-09-20@larluo{create}
 *   v1_1=2016-10-10@larluo{add hive table autogeneration}
 **********************************/

@GrabExclude('org.apache.hbase:hbase-client')
@GrabExclude('org.apache.hbase:hbase-annotations')
@GrabExclude('org.apache.hbase:hbase-hadoop2-compat')
@GrabExclude('org.apache.hbase:hbase-server')
@GrabExclude('org.apache.hbase:hbase-protocol')
@GrabExclude('org.apache.hbase:hbase-common')
@GrabExclude('org.apache.hbase:hbase-hadoop-compat')

@Grab('org.apache.thrift:libthrift:0.9.2')
@Grab('org.apache.hive:hive-service:2.1.0')
@Grab('org.apache.logging.log4j:log4j-core:2.6.2')
@Grab('org.apache.logging.log4j:log4j-api:2.6.2')

@Grab('org.apache.spark:spark-core_2.11:2.0.0')
@Grab('org.apache.spark:spark-yarn_2.11:2.0.0')

import java.security.CodeSource
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.CompilationUnit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSClient
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkSubmit
import scala.Tuple2
import scala.collection.JavaConversions
import java.util.jar.JarOutputStream
import java.util.jar.JarEntry
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.ConsoleAppender
import org.apache.log4j.PatternLayout
import org.apache.log4j.DailyRollingFileAppender

import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TSaslClientTransport
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.hive.service.rpc.thrift.TCLIService
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.rpc.thrift.TOpenSessionReq
import org.apache.hive.service.rpc.thrift.TFetchResultsReq
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq
import org.apache.hive.service.rpc.thrift.TGetSchemasReq
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq
import org.apache.hive.service.rpc.thrift.TFetchOrientation
import org.apache.hive.service.auth.PlainSaslHelper


/**********************************
 * PARAMETER AREA
 **********************************/
def (job_id) = ["spark-etl.merge"]
def (tabname, merge_cols_str) = args

/**********************************
 * LOG INITIALIZATION
 **********************************/
def log = Logger.getInstance(getClass())
System.setOut(new PrintStream(System.out) { void print(String str) {log.info(str)} })
System.setErr(new PrintStream(System.err) { void print(String str) {log.info(str)} })
log.root.with {
    it.level = Level.INFO
    it.removeAllAppenders()
    log.root.addAppender(new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")))
    log.root.addAppender(
      new DailyRollingFileAppender(
        new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"), 
        "/home/spiderdt/work/git/spiderdt-release/var/log/${job_id}.log",
        "'.'yyyy-MM-dd"
      )
    )
}
3.times {log.info ""}

/**********************************
 * CLASS LOADER SETUP
 **********************************/
class MyGroovyClassLoader extends GroovyClassLoader {
    CompilationUnit compilationUnit ;
    CompilationUnit createCompilationUnit(CompilerConfiguration config, CodeSource source) {
        compilationUnit = super.createCompilationUnit(config, source)
    }
}

/**********************************
 * PACKAGE JAR AND SUBMIT
 **********************************/
def jar_path = "/home/spiderdt/work/git/spiderdt-release/var/jar/${getClass().getName()}.jar"
new MyGroovyClassLoader().with {
    def bos = new ByteArrayOutputStream()
    def jar = new JarOutputStream(bos)
    it.parseClass(new File(getClass().protectionDomain.codeSource.location.path))
    it.compilationUnit.getClasses().each {
        jar.putNextEntry(new JarEntry(it.name + ".class"))
        jar.write(it.bytes)
    }
    jar.close()
    new File(jar_path).bytes = bos.toByteArray()
    bos.close()
}

SparkSubmit.main(
    ["--master", "yarn", 
     "--deploy-mode", "client", 
     "--class", "Client",
     "--executor-memory", "2g",
     jar_path,
     tabname, merge_cols_str
    ] as String[])

/**********************************
 * SPARK CODE
 **********************************/
class Client {
  static log = Logger.getInstance(Client.class)
  static list_dirs(dfs_client, filepath) {
      dfs_client.listPaths(filepath).getPartialListing().grep{it.dir}*.getFullName(filepath)
  }
  static list_leaf_dirs (dfs_client, filepath) {
      list_dirs(dfs_client, filepath).with {it ? it.collect{subDir -> list_leaf_dirs(dfs_client, subDir)}.flatten() : [filepath]} ?: filepath
  }
  static execute_hive_statement (hive_info, sql) {
      def transport = new TSaslClientTransport (
            'PLAIN',
            null,
            null,
            null,
            [:],
            new PlainSaslHelper.PlainCallbackHandler(hive_info.username ?: 'spiderdt', hive_info.password ?: 'spiderdt'),
            new TSocket(hive_info.hostname, 10000)
      )
      transport.open()
      def client = new TCLIService.Client(new TBinaryProtocol(transport))
      def sess_handle = client.OpenSession(new TOpenSessionReq()).sessionHandle
      def op_handle = client.ExecuteStatement(new TExecuteStatementReq(sess_handle, sql)).operationHandle
      def op_status = client.GetOperationStatus(new TGetOperationStatusReq(op_handle)).status.statusCode
      transport.close()
      op_status
  }
  static create_hive_tab (hive_info, tabname, prt_cols, data_cols) {
      log.info "HIVE TABLE:" + [tabname: tabname, prt_cols: prt_cols, data_cols: data_cols]
      def prt_cols_part = prt_cols ? ("PARTITIONED BY ( " + prt_cols.collect{"${it} STRING"}.join(", ") + " )") : ""
      def data_cols_part = data_cols.collect{"${it} STRING"}.join(", ")
      def ddl_sql = "CREATE EXTERNAL TABLE IF NOT EXISTS ${tabname} ( ${data_cols_part} ) ${prt_cols_part}"
      log.info "RUN DDL: " + ddl_sql
      execute_hive_statement(hive_info, ddl_sql)
  }
  static main(args) {
    // 1. PARSE PARAMETER
    def (tabname, merge_cols_str) = args
    def (merge_cols_name, merge_cols_idx) = Eval.me(merge_cols_str.replace(":", "")).with { [it*.get(0), it*.get(1)] }
    def (default_fs, username, uuid, hive_dir) = ["hdfs://192.168.1.3:9000", System.getProperty("user.name"), UUID.randomUUID(), "user/hive/warehouse"]
    log.info "params:" +  [tabname: tabname, merge_cols_name: merge_cols_name, merge_cols_idx: merge_cols_idx, uuid: uuid]

    // 2. RUN SPARK
    def sc = new JavaSparkContext (
        new SparkConf().with {
            it.setAll(JavaConversions.mapAsScalaMap(
                ["spark.app.name":"spark-etl.merge",
                 "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive": "true",
                 "spark.hadoop.yarn.resourcemanager.hostname": "192.168.1.3",
                 "spark.hadoop.fs.defaultFS": default_fs,
                 "spark.yarn.archive": default_fs + "/user/spiderdt/spark_yarn_archive"
            ))
        it}
    )
    // 2.1 CREATE SPARK HIVE TABLE
    def stg_header_str = sc.wholeTextFiles("/${hive_dir}/stg.db/${tabname}").map{it._2.split("\n")[0]}.collect().toList()
    if (stg_header_str.size() != 1) throw new Exception("data header didn't match!")
    create_hive_tab([hostname:"192.168.1.2"], "ods.${tabname}", [], stg_header_str[0].split(","))

    // 2.2 MERGE STG DATA TO ODS
    def stgRdd = sc.wholeTextFiles("/${hive_dir}/stg.db/${tabname}")
                   .flatMapToPair{
                     it._2.split("\n").drop(1).collect{it.split(",").with { new Tuple2(it[merge_cols_idx], it.join("\001")) }}.iterator()
                   }
    def odsRdd  = sc.wholeTextFiles("/${hive_dir}/ods.db/${tabname}")
                    .flatMapToPair{
                      it._2.split("\n").collect{ new Tuple2(it.split("\001")[merge_cols_idx], it) }.iterator()
                    }

    stgRdd.fullOuterJoin(odsRdd).map{
      it._2._1.orNull() ?: it._2._2.orNull()
    }.saveAsTextFile("${uuid}/${hive_dir}/ods.db/${tabname}")

    // 3. PERSIST DATA
    def dfs_client = new DFSClient(new URI(default_fs), new Configuration())
    list_leaf_dirs(dfs_client, "/user/${username}/${uuid}/${hive_dir}/ods.db/${tabname}").each {
        def tgt_path=it.replaceAll("^/user/${username}/${uuid}", "")
        log.info "persist to directory: ${tgt_path}"
        dfs_client.mkdirs(tgt_path.replaceAll('(.*)/[^/]*', '$1'))
        dfs_client.delete(tgt_path)
        dfs_client.rename(it, tgt_path)
    }
    dfs_client.delete("/user/${username}/${uuid}")
  }
}

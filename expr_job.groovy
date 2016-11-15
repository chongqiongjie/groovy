#! /bin/groovy
/*--------------------------------------
 * @[chong, larluo]
 * 2016-09-12    create
 * 2016-09-12    add shell command call
 *--------------------------------------
 * purpose: call clojure jar to calcluate the model expression
 * [prog] [job_id] [classified_json]
 * job_id =>
 * classified_json =>
 *--------------------------------------
*/

@Grab('log4j:log4j:1.2.17')
import groovy.json.JsonSlurper
import groovy.util.logging.Log4j
import org.apache.log4j.*

def (job_id, classified_json) = args

Logger log = Logger.getInstance(getClass())
System.setOut(new PrintStream(System.out) { void print(String str) {log.info(str)} })
System.setErr(new PrintStream(System.err) { void print(String str) {log.info(str)} })
log.root.with {
    it.level = Level.DEBUG
    it.removeAllAppenders()
    // log.root.addAppender(new ConsoleAppender(new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n")))
    log.root.addAppender(
      new DailyRollingFileAppender(
        new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n"), 
        "/home/spiderdt/work/git/spiderdt-release/var/log/${job_id}.log",
        "'.'yyyy-MM-dd"
      )
    )
}
log.info "\n\n\n"

def classified_info = new JsonSlurper().parseText(classified_json)
def classified_id = classified_info.classified_id ?: classified_info.classId // for ui compatiable

classified_info.data_sources.each{ data_source, columns_info ->
    def tabs_info  = [":model." + data_source, ":4ml." + classified_id + "." + data_source].toString()
    log.info "tabs_info: $tabs_info"

    def model_items = columns_info.driven_items*.data_item + columns_info.target_items*.data_item
    def cols_info = ([["'prt_path"]] + columns_info.column_items.collect{
        (it.data_type in ["string", "date"] ? "'" :  "") +  (model_items.contains(it.data_item) ? "" : "_")  + it.data_item
    }).toString()
    log.info "tabs_info: $cols_info"

    def exprs_info = columns_info.collect{ _item_type, item_infos ->
        item_infos.grep{it.expr}
    }.flatten().collect{[":" + it.data_item, '"' + it.expr + '"']}.toString()
    log.info "tabs_info: $exprs_info"

    log.info "JOB_ID: ${job_id} RUNNING..."
    def spark_cmd_str = [
        "/usr/lib/jvm/java-8-oracle/bin/java",
        "-cp", "/home/spiderdt/work/git/spiderdt-env/cluster/tarball/spark/conf/:/home/spiderdt/work/git/spiderdt-env/cluster/tarball/spark/jars/*",
        "-Xmx1g",
        "org.apache.spark.deploy.SparkSubmit",
        "--deploy-mode", "client",
        "--class", "spark_etl.expr" ,
        "/home/spiderdt/work/git/clojure-example/spark-etl/target/spark-etl-0.1.0-SNAPSHOT-standalone.jar",
        tabs_info, cols_info, exprs_info
    ]
    def spark_cmd = spark_cmd_str.execute(
        ['YARN_CONF_DIR=/home/spiderdt/work/git/spiderdt-env/cluster/conf/yarn',
         'SPARK_HOME=/home/spiderdt/work/git/spiderdt-env/cluster/tarball/spark'
        ], 
        new File('.')
    ).with {
        it.err.eachLine { log.info(it) }
        it.in.eachLine { log.info(it) }
        it
    }
    if (spark_cmd.exitValue()) {
        log.error "SPARK COMMAND RUN ERROR!"
        System.exit(-1)
    }
    log.info "JOB_ID: ${job_id} DONE!"
}

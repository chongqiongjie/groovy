#! /bin/groovy
/*--------------------------------------
 * @[dll]
 * 2016-09-14    create
 * 2016-09-18    updata
 *--------------------------------------
 * purpose:run a job list in intersect
 * [job_id] [classified_json]
 * job_id =>
 * classified_json =>
 *--------------------------------------
*/



@Grab('log4j:log4j:1.2.17')
import groovy.util.logging.Log4j
import org.apache.log4j.*
import groovy.json.JsonSlurper
	
	def (job_id, classified_json) = args
	
	Logger log = Logger.getInstance(getClass())
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
	
        def (jobId, classfied,pred_start_dt, pred_end_dt) = (job_id =~ /[^-_]+-([^-_]+)_([^-_]+)_([^-_]+)_.*/)[0]
        def   classified_info=new JsonSlurper().parseText(classified_json)
        log.info("job_id"+job_id)
        def   classified_id=classified_info.classId ?:classified_info.classified_id
        log.info("classified_id:"+classified_id)
        def   date_info=[['PredStartDate',pred_start_dt,'Date'],['PredEndDate',pred_end_dt,'Date']]
       
        classified_info.data_sources.each{data_source,columns_info->
        def itemMap=(columns_info.column_items+(columns_info.driven_items+columns_info.target_items).grep{it.expr}).collect{[it.data_item,(it.data_type ?:"string")]}
        def intersectMap=((columns_info.driven_items+columns_info.target_items).collect{[it.data_item,(it.data_type ?:"string" )]}).unique()
        def resultMap= itemMap.intersect(intersectMap)
	def columns_result=resultMap.collect{_item,_type->
            [(columns_info.target_items.data_item.contains(_item) ? "TargetCol":"InputCol") +","+_item+","+_type]
        }
			
	log.info("itemMap:"+itemMap)
	log.info("intersectMap:"+intersectMap)       
        log.info("resultmap:"+resultMap)      
        log.info("columns_result:"+columns_result)
	 
       // def data_path=data_source.substring(data_source.indexOf('.')+1)
       // println(data_path)
	def input_path="/home/spiderdt/work/git/spiderdt-release/var/data/in/${classified_id}/${data_source}/data.csv"
        def output_path="/home/spiderdt/work/git/spiderdt-release/var/data/out/${classified_id}/data.csv"		
        def input_info=[["InputDataPath",input_path,"string"]]
        def output_info=[["OutDataPath",output_path,"string"]]
        def result_info=columns_result+date_info+input_info+output_info
        def result_str=result_info.collect{it.join(",")}.join("\n")
	    new File("/home/spiderdt/work/git/spiderdt-release/var/conf/${job_id}.conf").text=result_str
	    
        def shell_str = ["sh", "-c", "/home/spiderdt/work/git/spiderdt-env/cluster/tarball/hadoop/bin/hdfs   dfs  -cat /user/hive/warehouse/4ml.db/${classified_id}/${data_source}/*/* >> ${input_path}"]
        def shell_cmd=shell_str.execute().with{
		it.err.eachLine { log.info(it) }
                it.in.eachLine { log.info(it) }
                it
		}
	    if (shell_cmd.exitValue()) {
        log.error "HDFS DFS -GETMERGE RUN ERROR!"
        System.exit(-1)
        }
        log.info "HDFS DFS -GETMERGE   DONE OK!"
        println(result_str)
        log.info(result_str)
        def shell_r="/home/spiderdt/work/git/spiderdt-release/data-platform/R/classified.R "
        def shell_rcmd=shell_r.execute().with{
                it.err.eachLine { log.info(it) }
                it.in.eachLine { log.info(it) }
                it
                }
        if (shell_rcmd.exitValue()) {
        log.error "classified.R RUN ERROR!"
        System.exit(-1)
        }
        log.info "classified.R   DONE OK!"
    }

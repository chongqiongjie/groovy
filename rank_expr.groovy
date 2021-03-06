@Grab('org.apache.poi:poi:3.15')
@Grab('org.apache.poi:poi-ooxml:3.15')
@Grab('mysql:mysql-connector-java:6.0.3')


import org.apache.poi.xssf.usermodel.XSSFWorkbook
import static org.apache.poi.ss.usermodel.Cell.*
import org.apache.poi.ss.usermodel.CellType

import java.nio.file.Paths
import groovy.time.TimeCategory

import static groovy.json.JsonOutput.toJson
import groovy.sql.Sql

getClass().classLoader.rootLoader.addURL(new URL("file:///home/spiderdt/.m2/repository/mysql/mysql-connector-java/6.0.3/mysql-connector-java-6.0.3.jar"))

def parse_xls(xls_path, sheetname) {
  Paths.get(xls_path).withInputStream { input ->
    def score_sheet = new XSSFWorkbook(input).getSheet(sheetname)
    def header = score_sheet.getRow(0).cellIterator()*.stringCellValue.takeWhile{it != "_"}
    [["bottler_cn", "bottler_en", *header], 
     score_sheet.rowIterator().drop(3).collect{ 
        it.cellIterator().collect{ [(it.cellTypeEnum in  [CellType.NUMERIC]) ? it.numericCellValue.toString() : it.stringCellValue, it.address.column] }
     }]
  }
}

def to_map(dataset) {
    def (header, matrix) = dataset
    matrix.collect {
      it.collectEntries{ val, idx -> [header[idx], val] }
    }
}
def kpi_ly(kpi_date)  { 
  def (kpi, date) = kpi_date.split("_", 2) 
  kpi + "_Dec" + use (TimeCategory) { (Date.parse( 'MMMyy', date) - 12.month).format('yy') }
}
def kpi_pp(kpi_date) { 
  def (kpi, date) = kpi_date.split("_", 2) 
  kpi + "_" + use (TimeCategory) { (Date.parse( 'MMMyy', date) - 1.month).format('MMMyy') }
}
def kpi_tb(kpi_date) {
  def (kpi,date) = kpi_date.split("_", 2)
  kpi + "_" + use (TimeCategory) {(Date.parse( 'MMMyy', date) - 12.month).format('MMMyy')} 

}

def lookup_cols(datamap) {
    datamap.collect {row ->"_"
      row.collectEntries {k, v ->
        k in ['bottler_cn', 'bottler_en'] ? [k,v] : [k, [v, v - (row[kpi_pp(k)] ?: 0), v - (row[kpi_ly(k)] ?: 0), v - (row[kpi_tb(k)] ?: 0)]]
      }
    }
}
def explode_cols(datamap, bottler_group) { 
  def botter_group_mapping = bottler_group.collectMany{it.value.collect{a_value-> [a_value, it.key]}}.collectEntries{it}
  datamap.collectMany { 
    def (bottler, channel) = it['bottler_en'].split("_", 2)
    it.findAll{!(it.key in ['bottler_cn', 'bottler_en'])}.collect {k, v ->
      def (kpi, date) = k.split("_", 2)
      [Date.parse( 'MMMyy', date).format('YYYY-MM'), botter_group_mapping[bottler], bottler, channel, kpi, *v]
    }
  }
}

def cal_expr(dataset, exprs) {
    def ret = [:]
    dataset.each {row -> 
        //println("row:" + row)
        exprs.each {
            if(it['dimension'].grep{it[0][0] != ':'}.every{ row[it[1]] == it[0] }) {
                def rpt_seg =  it['dimension'].grep{it[0][0] == ':'}.collect{[it[0].drop(1), it[1]]}
                def selector_val = it['selector'].collect{[it[0].drop(1), row[it[1]]]}
                println("selector:" + selector_val)
                def dimension_val = it['dimension'].grep{it[0][0] == ':'}.collect{[it[0].drop(1), row[it[1]]]}
                def metrics_val = it['metrics'].collectEntries{[it[0].drop(1), row[it[1]]]}

                def selector_map = [(selector_val): (ret?.get(rpt_seg)?.get(selector_val) ?: []) + [[dimension: dimension_val, metrics_val: metrics_val]]]
                def seg_map = [(rpt_seg): (ret?.get(rpt_seg) ?: [:]) + selector_map]
                ret += seg_map


            
            }
        }
    }
    ret
}


def tree(data) {
  data.groupBy{it.size() == 1}.collectEntries {
    it.key ? [items : it.value.collectMany{it}] : it.value.groupBy{it[0]}.collectEntries { [it.key, tree(it.value*.drop(1))] }
  }
}



def to_mysql(category_map, mysql_info) {
  println("[info] writing to mysql...")
  def sql = Sql.newInstance(mysql_info)
  sql.execute """CREATE TABLE IF NOT EXISTS cocacola_rpt_chong ( 
                   category VARCHAR(64), 
                   name VARCHAR(256), 
                   selector VARCHAR(512), 
                   data VARCHAR(10000),
                   CONSTRAINT pk_rpt_chong PRIMARY KEY(name, category, selector)
                 ) ;"""
   println("[info] create table success")
   sql.withBatch(10000, "REPLACE INTO cocacola_rpt_chong(category, name, selector, data) VALUES('score', ?, ?, ?)") {
    category_map.each {rpt_seg, selector_map->
      selector_map.each {selector, data ->
        def tree_data = tree(data.collect{it.dimension + it.metrics_val})


        println( "rpt_seg:" + rpt_seg)
        println( "selector:" + toJson(selector))
        println( "tree_data:" + toJson(tree_data)) 
        it.addBatch(rpt_seg*.get(0).join("_") ?: "overall", toJson(selector), toJson(tree_data))
      }
    }
  }
}



 cocacola_xls = "/home/spiderdt/work/git/spiderdt-working/larluo/score.xlsm"

// date, bottler_group, bottler, channel, kpi, score, score_pp, score_lp,score_tb

def exprs = [[ selector : [[":Bottler", 2], [":Channel", 3], [":KPI",4]], 
               dimension : [[":date", 0]],
               metrics : [[":score", 5],[":score_tb",8]] 
            ]]
def mysql_info = [url: "jdbc:mysql://192.168.1.2:3306/state_store?useSSL=false",
                  user: "state_store",
                  password: "spiderdt",
                  driver: "com.mysql.cj.jdbc.Driver"
                  ]
def bottler_group = [China : ['China'],
                     BIG: ['BIG','LNS','GX','YN','Shanxi','LNN','SH','HB','SC','CQ','HLJ','JL'],
                     CBL: ['CBL','HaiN','BJ','ZM','SD','HeB','JX','TJ','HuN','InnM','GS','XJ'],
                     SBL: ['SBL','AH','ZJ','FJ','Shaanxi','HeN','JS','GDW','GDE'],
                     Zhuhai: ['ZH']
                    ]
cocacola_xls.with{ parse_xls(it, "score") }
            .with{ to_map(it) }
            .with{ lookup_cols(it) }
            .with{ explode_cols(it, bottler_group) }
            .with{ cal_expr(it, exprs) }
            .with{ to_mysql(it, mysql_info) }

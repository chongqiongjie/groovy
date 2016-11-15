#! /bin/groovy

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

def data_cols=["larluoid1", "larluoid2"]
def prt_cols=["p_date", "p_code"]

def data_cols_part = data_cols.collect{"${it} STRING"}.join(", ")
def prt_cols_part = prt_cols ? ("PARTITIONED BY ( " + prt_cols.collect{"${it} STRING"}.join(", ") + " )") : ""
def ddl_sql = "CREATE EXTERNAL TABLE IF NOT EXISTS ods.larluo ( " + data_cols_part + " ) " + prt_cols_part ;

def transport = new TSaslClientTransport (
      'PLAIN',
      null,
      null,
      null,
      [:],
      new PlainSaslHelper.PlainCallbackHandler('spiderdt', 'spiderdt'),
      new TSocket('192.168.1.2', 10000)
)
transport.open()
def client = new TCLIService.Client(new TBinaryProtocol(transport))
def sess_handle = client.OpenSession(new TOpenSessionReq()).sessionHandle
               
println("ddl_sql:" + ddl_sql)
def op_handle = client.ExecuteStatement(new TExecuteStatementReq(sess_handle, ddl_sql)).operationHandle
def op_status = client.GetOperationStatus(new TGetOperationStatusReq(op_handle)).status.statusCode
transport.close()
println(op_status)

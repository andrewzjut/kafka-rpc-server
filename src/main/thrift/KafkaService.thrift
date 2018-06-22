
namespace java com.trcloud.thrift.service


enum Status
{
  SUCCESS,           /*成功*/
  ERROR,             /*失败*/
  EXCEPTION,	 	 /*内部出现异常*/
}

exception ThriftException
{
  1:string message,
}

struct Result
{
  1: Status status,
  2: string message,
  3: ThriftException thriftException,
}

service  KafkaService {  
  Result sendMessage(1:string topic,2:string value)
}
import com.caucho.hessian.io.Hessian2Output;
import com.caucho.hessian.io.Hessian2Input;
import com.xxl.rpc.remoting.net.params.XxlRpcRequest;
import com.xxl.rpc.remoting.net.params.XxlRpcResponse;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.biz.model.ReturnT;
import java.io.*;
import java.util.HexFormat;
public class XxlVector {
 static byte[] encode(Object o)throws Exception{ByteArrayOutputStream b=new ByteArrayOutputStream();Hessian2Output h=new Hessian2Output(b);h.writeObject(o);h.flush();return b.toByteArray();}
 static void print(String name,Object o)throws Exception{System.out.println(name+"="+HexFormat.of().formatHex(encode(o)));}
 public static void main(String[] a)throws Exception{
  XxlRpcRequest beat=new XxlRpcRequest();beat.setRequestId("req-beat");beat.setCreateMillisTime(1700000000000L);beat.setAccessToken("secret");beat.setClassName(ExecutorBiz.class.getName());beat.setMethodName("beat");beat.setParameterTypes(new Class<?>[]{});beat.setParameters(new Object[]{});beat.setVersion("");print("beat",beat);
  TriggerParam p=new TriggerParam();p.setJobId(7);p.setExecutorHandler("demo");p.setExecutorParams("hello");p.setExecutorBlockStrategy("SERIAL_EXECUTION");p.setGlueType("BEAN");p.setLogId(123);p.setLogDateTime(1700000000000L);
  XxlRpcRequest run=new XxlRpcRequest();run.setRequestId("req-run");run.setCreateMillisTime(1700000000000L);run.setAccessToken("secret");run.setClassName(ExecutorBiz.class.getName());run.setMethodName("run");run.setParameterTypes(new Class<?>[]{TriggerParam.class});run.setParameters(new Object[]{p});run.setVersion("");print("run",run);
  XxlRpcResponse response=new XxlRpcResponse();response.setRequestId("req-beat");response.setResult(ReturnT.SUCCESS);print("response",response);
  if(a.length>0){byte[] raw=HexFormat.of().parseHex(a[0]);Hessian2Input h=new Hessian2Input(new ByteArrayInputStream(raw));Object decoded=h.readObject();System.out.println("decoded="+decoded.getClass()+":"+decoded);}
 }
}

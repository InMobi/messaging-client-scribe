package random.pkg;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;

import scribe.thrift.LogEntry;
import scribe.thrift.ResultCode;
import scribe.thrift.scribe.Iface;

public class ScribeSlacker implements Iface {
	
	private Logger logger;

	public ScribeSlacker() {
		logger = Logger.getLogger("scribeserver");
	}


	@Override
	public String getName() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getVersion() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public fb_status getStatus() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStatusDetails() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Long> getCounters() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getCounter(String key) throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setOption(String key, String value) throws TException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getOption(String key) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, String> getOptions() throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getCpuProfile(int profileDurationInSec) throws TException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long aliveSince() throws TException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void reinitialize() throws TException {
		// TODO Auto-generated method stub

	}

	@Override
	public void shutdown() throws TException {
		// TODO Auto-generated method stub

	}

	@Override
	public ResultCode Log(List<LogEntry> messages) throws TException {
		try {
			System.err.println("server is slacking");
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ResultCode.OK;
	}

}

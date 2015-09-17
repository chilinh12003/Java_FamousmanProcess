package pro.charge;

import java.io.StringReader;
import java.util.Calendar;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import pro.server.CurrentData;
import pro.server.LocalConfig;
import uti.utility.MyConfig;
import uti.utility.MyLogger;
import dat.history.ChargeLog;
import dat.sub.SubscriberObject;
import db.define.MyDataRow;
import db.define.MyTableModel;

public class Charge
{
	public enum Reason
	{
		Nothing(0), REG(1), RENEW(2), UNREG(3), CONTENT(4), ;

		private int value;

		private Reason(int value)
		{
			this.value = value;
		}

		public int GetValue()
		{
			return this.value;
		}

		public static Reason FromInt(int iValue)
		{
			for (Reason type : Reason.values())
			{
				if (type.GetValue() == iValue) return type;
			}
			return Nothing;
		}
	}

	public enum ErrorCode
	{
		ChargeSuccess(0), BlanceTooLow(1), WrongUserAndPassword(2), ChargeNotComplete(3), OtherError(4), WrongSubscriberNumber(
				5), SubDoesNotExist(6), OverChargeLimit(7), OverChargeLimit2(17), ServerInternalError(8), ConfigError(9), RequestIDIsNull(
				10), UnknowIP(99), SynctaxXMLError(100), UnknownRequest(500), InvalidSubscriptionState(11), UnknowError(
				9999), VNPAPIError(-1);

		private int value;

		private ErrorCode(int value)
		{
			this.value = value;
		}

		public Integer GetValue()
		{
			return this.value;
		}

		public static ErrorCode FromInt(int iValue)
		{
			for (ErrorCode type : ErrorCode.values())
			{
				if (type.GetValue() == iValue) return type;
			}
			return UnknowError;
		}
	}

	/**
	 * cho phep insert charglog khi charge xong cho 1 so dien thoai
	 */
	public boolean AllowInsertChargeLog = true;

	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());

	public ErrorCode ChargeRegFree(SubscriberObject mObject, MyConfig.ChannelType mChannel, String Note)
			throws Exception
	{
		try
		{
			Integer RetryCount = 0;
			ErrorCode mResult = ErrorCode.VNPAPIError;
			while (mResult == ErrorCode.VNPAPIError && RetryCount++ <= LocalConfig.CHARGE_MAX_ERROR_RETRY)
			{
				mResult = ChargeVNP(mObject, 0, Reason.REG, 2000, 1, Note, mChannel,
						RetryCount);
				if (mResult == ErrorCode.VNPAPIError) Thread.sleep(1000);
			}
			return mResult;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public ErrorCode ChargeReg(SubscriberObject mObject, MyConfig.ChannelType mChannel, String Note) throws Exception
	{
		try
		{
			Integer RetryCount = 0;
			ErrorCode mResult = ErrorCode.VNPAPIError;
			while (mResult == ErrorCode.VNPAPIError && RetryCount++ <= LocalConfig.CHARGE_MAX_ERROR_RETRY)
			{
				mResult = ChargeVNP(mObject, 2000, Reason.REG, 2000, 0, Note, mChannel,
						RetryCount);
				if (mResult == ErrorCode.VNPAPIError) Thread.sleep(1000);
			}

			return mResult;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public ErrorCode ChargeRenew(SubscriberObject mObject, MyConfig.ChannelType mChannel, String Note) throws Exception
	{
		try
		{
			Integer RetryCount = 0;
			ErrorCode mResult = ErrorCode.VNPAPIError;
			while (mResult == ErrorCode.VNPAPIError && RetryCount++ <= LocalConfig.CHARGE_MAX_ERROR_RETRY)
			{
				mResult = ChargeVNP(mObject, 2000, Reason.RENEW, 2000, 0, Note, mChannel,
						RetryCount);
				if (mResult == ErrorCode.VNPAPIError) Thread.sleep(1000);
			}
			return mResult;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public ErrorCode ChargeDereg(SubscriberObject mObject, MyConfig.ChannelType mChannel, String Note) throws Exception
	{
		try
		{
			Integer RetryCount = 0;
			ErrorCode mResult = ErrorCode.VNPAPIError;
			while (mResult == ErrorCode.VNPAPIError && RetryCount++ <= LocalConfig.CHARGE_MAX_ERROR_RETRY)
			{
				mResult = ChargeVNP(mObject, 0, Reason.UNREG, 0, 0, Note, mChannel,
						RetryCount);
				if (mResult == ErrorCode.VNPAPIError) Thread.sleep(1000);
			}
			return mResult;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public ErrorCode ChargeBuyContent(SubscriberObject mObject, MyConfig.ChannelType mChannel, String Note)
			throws Exception
	{
		try
		{
			Integer RetryCount = 0;
			ErrorCode mResult = ErrorCode.VNPAPIError;
			while (mResult == ErrorCode.VNPAPIError && RetryCount++ <= LocalConfig.CHARGE_MAX_ERROR_RETRY)
			{
				mResult = ChargeVNP(mObject, 1000, Reason.CONTENT, 1000, 0, Note, mChannel,
						RetryCount);
				if (mResult == ErrorCode.VNPAPIError) Thread.sleep(1000);
			}
			return mResult;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	private ErrorCode GetErrorCode(String XML) throws Exception
	{
		ErrorCode Result = ErrorCode.VNPAPIError;
		try
		{
			DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(XML));

			Document doc = db.parse(is);
			NodeList nodes = doc.getElementsByTagName("ERROR");

			Element line = (Element) nodes.item(0);

			Node child = line.getFirstChild();

			if (child instanceof CharacterData)
			{
				CharacterData cd = (CharacterData) child;
				String Code = cd.getData();

				Result = ErrorCode.FromInt(Integer.parseInt(Code.trim()));
			}

		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
		return Result;
	}

	private boolean InsertChargeLog(SubscriberObject mObject, Integer PRICE, Reason REASON, Integer ORIGINALPRICE,
			Integer PROMOTION, MyConfig.ChannelType CHANNEL, ErrorCode mResult)
	{
		try
		{
			Calendar CurrentDate = Calendar.getInstance();

			ChargeLog mChargeLog = new ChargeLog(LocalConfig.mDBConfig_MSSQL);
			MyTableModel mTableLog = CurrentData.GetTable_ChargeLog();
			mTableLog.Clear();
			MyDataRow mRow_Log = mTableLog.CreateNewRow();
			
			mRow_Log.SetValueCell("MSISDN", mObject.MSISDN);
			mRow_Log.SetValueCell("ChargeDate", MyConfig.Get_DateFormat_InsertDB().format(CurrentDate.getTime()));
			mRow_Log.SetValueCell("Price", PRICE);
			mRow_Log.SetValueCell("ChargeTypeID", REASON.GetValue());
			mRow_Log.SetValueCell("ChargeTypeName", REASON.toString());
			mRow_Log.SetValueCell("ChargeStatusID", mResult.GetValue());
			mRow_Log.SetValueCell("ChargeStatusName", mResult.toString());
			mRow_Log.SetValueCell("PID", mObject.PID);
			mRow_Log.SetValueCell("ChannelTypeID", CHANNEL.GetValue());
			mRow_Log.SetValueCell("ChannelTypeName", CHANNEL.toString());
			mRow_Log.SetValueCell("PartnerID", mObject.PartnerID);
			
			mRow_Log.SetValueCell("AppID", mObject.mVNPApp.GetValue());
			mRow_Log.SetValueCell("AppName", mObject.mVNPApp.toString());
			mRow_Log.SetValueCell("UserName", mObject.UserName);
			mRow_Log.SetValueCell("IP", mObject.IP);
			
			mTableLog.AddNewRow(mRow_Log);
			return mChargeLog.Insert(0, mTableLog.GetXML());
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
			return false;
		}
	}

	private ErrorCode ChargeVNP(SubscriberObject mObject, Integer PRICE, Reason REASON, Integer ORIGINALPRICE,
			Integer PROMOTION, String NOTE, MyConfig.ChannelType CHANNEL, Integer RetryCount) throws Exception
	{
		String XMLResponse = "";
		String XMLReqeust = "";
		String RequestID = Long.toString(System.currentTimeMillis());
		String UserName = LocalConfig.VNPUserName;
		String Password = LocalConfig.VNPPassword;
		String CPName = LocalConfig.VNPCPName;
		ErrorCode mResult = ErrorCode.OtherError;

		String SaveChargeLogStatus = "Save Chargelog Satus: OK";
		try
		{
			String Template = "<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\" ?>"
					+ "<VAS request_id=\"%s\">" + "<REQ name=\"%s\" user=\"%s\" password=\"%s\">" + "<SUBSCRIBER>"
					+ "<SUBID>%s</SUBID>" + "<PRICE>%s</PRICE>" + "<REASON>%s</REASON>"
					+ "<ORIGINALPRICE>%s</ORIGINALPRICE>" + "<PROMOTION>%s</PROMOTION>" + "<NOTE>%s</NOTE>"
					+ "<CHANNEL>%s</CHANNEL>" + "</SUBSCRIBER>" + "</REQ>" + "</VAS>";

			XMLReqeust = String.format(Template, new Object[]
			{RequestID, CPName, UserName, Password, mObject.MSISDN, PRICE, REASON.toString(), ORIGINALPRICE, PROMOTION,
					NOTE, CHANNEL.toString()});

			HttpPost mPost = new HttpPost(LocalConfig.VNPURLCharging);

			StringEntity mEntity = new StringEntity(XMLReqeust, ContentType.create("text/xml", "UTF-8"));
			mPost.setEntity(mEntity);

			HttpClient mClient = new DefaultHttpClient();
			try
			{
				HttpResponse response = mClient.execute(mPost);
				HttpEntity entity = response.getEntity();
				XMLResponse = EntityUtils.toString(entity);
				EntityUtils.consume(entity);
			}
			catch (Exception ex)
			{
				throw ex;
			}
			finally
			{
				mClient.getConnectionManager().shutdown();
			}

			mResult = GetErrorCode(XMLResponse);

			if (AllowInsertChargeLog == false && REASON == Reason.RENEW)
			{
				// nếu trong thead charge renew thì kho insert xuong db. ma se
				// insert ngay trong thead charge để đảm bảo tốc độ

			}
			else
			{
				if (!InsertChargeLog(mObject, PRICE, REASON, ORIGINALPRICE, PROMOTION, CHANNEL, mResult))
				{
					SaveChargeLogStatus = "Save Chargelog Satus: FAIL";
				}
			}
		}
		catch (Exception ex)
		{
			mResult = ErrorCode.VNPAPIError;
			mLog.log.error(ex);
		}
		finally
		{
			MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_VNP", "REQUEST CHARGE --> " + XMLReqeust
					+ " || RetryCount:" + RetryCount.toString());
			MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_VNP", "RESPONSE CHARGE --> " + XMLResponse
					+ " || " + SaveChargeLogStatus + " | Result:" + mResult.toString());
		}

		return mResult;
	}
}

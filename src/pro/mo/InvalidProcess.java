package pro.mo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import pro.server.Common;
import pro.server.ContentAbstract;
import pro.server.Keyword;
import pro.server.LocalConfig;
import pro.server.MsgObject;
import uti.utility.MyConfig.ChannelType;
import uti.utility.VNPApplication;
import uti.utility.MyConvert;
import uti.utility.MyLogger;
import dat.content.DefineMT;
import dat.content.DefineMT.MTType;
import dat.history.MOLog;
import dat.history.MOObject;
import dat.sub.SubscriberObject;
import db.define.MyTableModel;

/**
 * 
 * @author Administrator
 * 
 */
public class InvalidProcess  extends ContentAbstract
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());
	Collection<MsgObject> ListMessOject = new ArrayList<MsgObject>();

	MsgObject mMsgObject = null;
	SubscriberObject mSubObj = new SubscriberObject();

	Calendar mCal_Current = Calendar.getInstance();

	MOLog mMOLog = null;

	MyTableModel mTable_MOLog = null;

	DefineMT.MTType mMTType = MTType.Invalid;

	private void Init(MsgObject msgObject, Keyword keyword) throws Exception
	{
		try
		{
			mMOLog = new MOLog(LocalConfig.mDBConfig_MSSQL);
			mTable_MOLog = mMOLog.Select(0);
			mMsgObject = msgObject;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	private Collection<MsgObject> AddToList() throws Exception
	{
		try
		{
			ListMessOject.clear();
			String MTContent_Current = Common.GetDefineMT_Message(mMTType);
			if (!MTContent_Current.equalsIgnoreCase(""))
			{
				// nếu đăng ký mới thành công, thì thêm 3 MT Notify vào
				mMsgObject.setUsertext(MTContent_Current);
				mMsgObject.setContenttype(LocalConfig.LONG_MESSAGE_CONTENT_TYPE);
				mMsgObject.setMsgtype(1);
				ListMessOject.add(new MsgObject((MsgObject) mMsgObject.clone()));
				AddToMOLog(mMTType, MTContent_Current);
			}
			return ListMessOject;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	private void AddToMOLog(MTType mMTType_Current, String MTContent_Current) throws Exception
	{
		try
		{
			MOObject mMOObj = new MOObject(mMsgObject.getUserid(), ChannelType.FromInt(mMsgObject.getChannelType()),
					mMTType_Current, mMsgObject.getMO(), MTContent_Current, mMsgObject.getRequestid().toString(),
					MyConvert.GetPIDByMSISDN(mMsgObject.getUserid(), LocalConfig.MAX_PID), mMsgObject.getReceiveDate(),
					Calendar.getInstance().getTime(), new VNPApplication(), null, null, 0);

			mTable_MOLog = mMOObj.AddNewRow(mTable_MOLog);

		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	private void Insert_MOLog() throws Exception
	{
		try
		{
			MOLog mMOLog = new MOLog(LocalConfig.mDBConfig_MSSQL);
			mMOLog.Insert(0, mTable_MOLog.GetXML());
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	protected Collection<MsgObject> getMessages(MsgObject msgObject, Keyword keyword) throws Exception
	{
		try
		{
			Init(msgObject, keyword);			
			return AddToList();
		}
		catch (Exception ex)
		{
			mLog.log.error(Common.GetStringLog(msgObject), ex);
			mMTType = MTType.SystemError;
			return AddToList();
		}
		finally
		{
			mLog.log.debug(Common.GetStringLog(mMsgObject));
			Insert_MOLog();
		}
	}

}
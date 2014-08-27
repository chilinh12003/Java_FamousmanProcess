package pro.mo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import pro.charge.Charge;
import pro.charge.Charge.ErrorCode;
import pro.server.Common;
import pro.server.ContentAbstract;
import pro.server.CurrentData;
import pro.server.Keyword;
import pro.server.LocalConfig;
import pro.server.MsgObject;
import uti.utility.MyConfig.ChannelType;
import uti.utility.MyConfig.VNPApplication;
import uti.utility.MyConvert;
import uti.utility.MyLogger;
import dat.content.DefineMT;
import dat.content.DefineMT.MTType;
import dat.history.MOLog;
import dat.history.MOObject;
import dat.sub.Subscriber;
import dat.sub.SubscriberObject;
import dat.sub.UnSubscriber;
import db.define.MyTableModel;

public class Deregister extends ContentAbstract
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());
	Collection<MsgObject> ListMessOject = new ArrayList<MsgObject>();

	MsgObject mMsgObject = null;
	SubscriberObject mSubObj = new SubscriberObject();

	Calendar mCal_Current = Calendar.getInstance();

	Subscriber mSub = null;
	UnSubscriber mUnSub = null;
	MOLog mMOLog = null;

	MyTableModel mTable_MOLog = null;
	MyTableModel mTable_Sub = null;
	MyTableModel mTable_UnSub = null;

	DefineMT.MTType mMTType = MTType.RegFail;

	String MTContent = "";
	Integer PartnerID = 0;

	pro.charge.Charge mCharge = new Charge();

	private void Init(MsgObject msgObject, Keyword keyword) throws Exception
	{
		try
		{
			mSub = new Subscriber(LocalConfig.mDBConfig_MSSQL);
			mUnSub = new UnSubscriber(LocalConfig.mDBConfig_MSSQL);
			mMOLog = new MOLog(LocalConfig.mDBConfig_MSSQL);

			mTable_MOLog = CurrentData.GetTable_MOLog();
			mTable_Sub = CurrentData.GetTable_Sub();
			mTable_UnSub = CurrentData.GetTable_UnSub();

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
			MTContent = Common.GetDefineMT_Message(mMTType);
			if (!MTContent.equalsIgnoreCase(""))
			{
				mMsgObject.setUsertext(MTContent);
				mMsgObject.setContenttype(LocalConfig.LONG_MESSAGE_CONTENT_TYPE);
				mMsgObject.setMsgtype(1);

				ListMessOject.add(new MsgObject(mMsgObject));
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
					Calendar.getInstance().getTime(), VNPApplication.NoThing, null, null, mSubObj.PartnerID);

			mTable_MOLog = mMOObj.AddNewRow(mTable_MOLog);

		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	private void Insert_MOLog()
	{
		try
		{
			mMOLog.Insert(0, mTable_MOLog.GetXML());
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	private boolean MoveToSub() throws Exception
	{
		try
		{
			mTable_UnSub.Clear();
			mTable_UnSub = mSubObj.AddNewRow(mTable_UnSub);

			if (!mUnSub.Move(0, mTable_UnSub.GetXML()))
			{
				mLog.log.info(" Move Tu Sub Sang UnSub KHONG THANH CONG: XML Insert-->" + mTable_UnSub.GetXML());
				return false;
			}

			return true;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	protected void CreateUnSub() throws Exception
	{
		mSubObj.mChannelType = ChannelType.FromInt(mMsgObject.getChannelType());
		mSubObj.DeregDate = mCal_Current.getTime();
	}

	protected Collection<MsgObject> getMessages(MsgObject msgObject, Keyword keyword) throws Exception
	{
		try
		{
			// Khoi tao
			Init(msgObject, keyword);

			Integer PID = MyConvert.GetPIDByMSISDN(mMsgObject.getUserid(), LocalConfig.MAX_PID);

			MyTableModel mTable_Sub = mSub.Select(2, PID.toString(), mMsgObject.getUserid());

			if (mTable_Sub.GetRowCount() > 0) 
				mSubObj = SubscriberObject.Convert(mTable_Sub, false);

			mSubObj.PID = PID;

			// Nếu chưa đăng ký dịch vụ
			if (mSubObj.IsNull())
			{
				mMTType = MTType.DeregNotRegister;
				return AddToList();
			}		

			CreateUnSub();

			if (ErrorCode.ChargeSuccess != mCharge.ChargeDereg(mSubObj,mSubObj.mChannelType,"HUY"))
			{
				MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_Sync_Dereg_VNP_FAIL", "DEREG FROM SMS --> "
						+ Common.GetStringLog(mMsgObject));
			}

			if (MoveToSub())
			{
				mMTType = MTType.DeregSuccess;
				return AddToList();
			}

			mMTType = MTType.DeregFail;

			return AddToList();
		}
		catch (Exception ex)
		{
			mLog.log.error(Common.GetStringLog(msgObject), ex);
			mMTType = MTType.DeregFail;
			return AddToList();
		}
		finally
		{
			AddToMOLog(mMTType, MTContent);

			// Insert vao log
			Insert_MOLog();

			mLog.log.debug(Common.GetStringLog(mMsgObject));
		}
	}

}

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
import dat.content.SuggestObject;
import dat.history.MOLog;
import dat.history.MOObject;
import dat.history.Play;
import dat.history.Play.PlayType;
import dat.history.PlayObject;
import dat.history.SuggestCount;
import dat.history.SuggestCountObject;
import dat.sub.Subscriber;
import dat.sub.Subscriber.Status;
import dat.sub.SubscriberObject;
import dat.sub.UnSubscriber;
import db.define.MyTableModel;

public class BuySuggest extends ContentAbstract
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());
	Collection<MsgObject> ListMessOject = new ArrayList<MsgObject>();

	MsgObject mMsgObject = null;
	SubscriberObject mSubObj = new SubscriberObject();

	SuggestObject mSuggestObj = new SuggestObject();
	SuggestCountObject mSuggestCountObj = new SuggestCountObject();

	Calendar mCal_Current = Calendar.getInstance();
	Calendar mCal_Begin = Calendar.getInstance();
	Calendar mCal_End = Calendar.getInstance();

	Subscriber mSub = null;
	UnSubscriber mUnSub = null;
	MOLog mMOLog = null;
	dat.content.Keyword mKeyword = null;

	MyTableModel mTable_MOLog = null;
	MyTableModel mTable_Sub = null;

	pro.charge.Charge mCharge = new Charge();
	DefineMT.MTType mMTType = MTType.BuySugFail;

	private void Init(MsgObject msgObject, Keyword keyword) throws Exception
	{
		try
		{
			mSub = new Subscriber(LocalConfig.mDBConfig_MSSQL);
			mUnSub = new UnSubscriber(LocalConfig.mDBConfig_MSSQL);
			mMOLog = new MOLog(LocalConfig.mDBConfig_MSSQL);
			mKeyword = new dat.content.Keyword(LocalConfig.mDBConfig_MSSQL);

			mTable_MOLog = CurrentData.GetTable_MOLog();
			mTable_Sub = CurrentData.GetTable_Sub();

			mMsgObject = msgObject;

			mCal_Begin.set(Calendar.MILLISECOND, 0);
			mCal_Begin.set(mCal_Current.get(Calendar.YEAR), mCal_Current.get(Calendar.MONTH),
					mCal_Current.get(Calendar.DATE), 8, 0, 0);

			mCal_End.set(Calendar.MILLISECOND, 0);
			mCal_End.set(mCal_Current.get(Calendar.YEAR), mCal_Current.get(Calendar.MONTH),
					mCal_Current.get(Calendar.DATE), 23, 59, 59);
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
			if (mMTType == MTType.BuySugSuccess)
			{
				if (mSuggestObj.MT != null && !mSuggestObj.MT.equalsIgnoreCase(""))
				{
					mMsgObject.setUsertext(mSuggestObj.MT);
					mMsgObject.setContenttype(LocalConfig.LONG_MESSAGE_CONTENT_TYPE);
					mMsgObject.setMsgtype(1);
					ListMessOject.add(new MsgObject((MsgObject) mMsgObject.clone()));
					AddToMOLog(mMTType, mSuggestObj.MT);
				}
				if (mSuggestObj.NotifyMT != null && !mSuggestObj.NotifyMT.equalsIgnoreCase(""))
				{
					Integer TotalAnswer = mSuggestCountObj.CorrectCount + mSuggestCountObj.IncorrectCount;
					
					String MTContent = mSuggestObj.NotifyMT;
					
					MTContent = MTContent.replace("[BuyCount]", Integer.toString(mSuggestCountObj.BuyCount));
					MTContent = MTContent.replace("[AnswerCount]", TotalAnswer.toString());
					mMsgObject.setUsertext(MTContent);
					mMsgObject.setContenttype(LocalConfig.LONG_MESSAGE_CONTENT_TYPE);
					mMsgObject.setMsgtype(1);
					ListMessOject.add(new MsgObject((MsgObject) mMsgObject.clone()));
					AddToMOLog(MTType.BuySugNotify , MTContent);
				}
			}
			else
			{
				String MTContent = Common.GetDefineMT_Message(mMTType);
				MTContent = MTContent.replace("[PlayDate]",CurrentData.Get_Current_QuestionObj().Get_PlayDate());
				MTContent = MTContent.replace("[NextDate]",CurrentData.Get_Current_QuestionObj().Get_NextDate());
				
				if (!MTContent.equalsIgnoreCase(""))
				{
					mMsgObject.setUsertext(MTContent);
					mMsgObject.setContenttype(LocalConfig.LONG_MESSAGE_CONTENT_TYPE);
					mMsgObject.setMsgtype(1);
					ListMessOject.add(new MsgObject((MsgObject) mMsgObject.clone()));
					AddToMOLog(mMTType, MTContent);
				}
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

	/**
	 * Lấy dữ kiện để trả về, đồng thời tạo các thông tin cần Update xuống DB
	 * 
	 * @throws Exception
	 */
	private void CreateUpdateSub() throws Exception
	{		
		mSubObj.LastSuggestDate = mCal_Current.getTime();
		mSubObj.LastSuggestrID = mSuggestObj.SuggestID;
		mSubObj.TotalSuggest++;
	}

	/**
	 * Thêm vào log Mua dữ kiện và trả lời
	 * 
	 * @return
	 */
	private boolean Insert_Play()
	{
		try
		{
			PlayObject mObject = new PlayObject();
			mObject.mPlayType = PlayType.BuySuggest;
			mObject.MSISDN = mSubObj.MSISDN;
			mObject.mStatus = Play.Status.BuySuggest;
			mObject.OrderNumber = mSuggestObj.OrderNumber;
			mObject.PID = mSubObj.PID;
			mObject.QuestionID = mSuggestObj.QuestionID;
			mObject.ReceiveDate = mMsgObject.getReceiveDate();
			mObject.SuggestID = mSuggestObj.SuggestID;

			MyTableModel mTable = CurrentData.GetTable_Play();

			mTable = mObject.AddNewRow(mTable);

			Play mPlay = new Play(LocalConfig.mDBConfig_MSSQL);
			return mPlay.Insert(0, mTable.GetXML());

		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
			return false;
		}
	}

	/**
	 * Cập nhật số lượng mua dữ kiện này trong ngày
	 * 
	 * @return
	 */
	private boolean Update_SuggestCount()
	{
		try
		{
			mSuggestCountObj = CurrentData.Get_SuggestCountObj(mSuggestObj);
			mSuggestCountObj.BuyCount++;
			mSuggestCountObj.LastUpdate = mCal_Current.getTime();

			MyTableModel mTable = CurrentData.GetTable_SuggestCount();

			mTable = mSuggestCountObj.AddNewRow(mTable);

			SuggestCount mSuggestCount = new SuggestCount(LocalConfig.mDBConfig_MSSQL);

			return mSuggestCount.Update(1, mTable.GetXML());

		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
			return false;
		}
	}

	/**
	 * Update thông tin mua dữ kiện vào Sub
	 * 
	 * @return
	 */
	private boolean UpdateSubInfo()
	{
		try
		{
			mTable_Sub.Clear();
			mTable_Sub = mSubObj.AddNewRow(mTable_Sub);

			if (!mSub.Update(1, mTable_Sub.GetXML()))
			{
				mLog.log.warn("Update Thong tin Mua du kien vao table Subscriber KHONG THANH CONG: XML Insert-->"
						+ mTable_Sub.GetXML());
				return false;
			}

			return true;
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
			return false;
		}

	}

	protected Collection<MsgObject> getMessages(MsgObject msgObject, Keyword keyword) throws Exception
	{
		try
		{
			// Khoi tao
			Init(msgObject, keyword);

			Integer PID = MyConvert.GetPIDByMSISDN(mMsgObject.getUserid(), LocalConfig.MAX_PID);

			// Lấy thông tin khách hàng đã đăng ký
			MyTableModel mTable_Sub = mSub.Select(2, PID.toString(), mMsgObject.getUserid());

			mSubObj = SubscriberObject.Convert(mTable_Sub, false);

			// Chưa đăng ký
			if (mSubObj.IsNull())
			{
				mMTType = MTType.BuySugNotReg;
				return AddToList();
			}

			// Tình trạng không hợp lệ
			if (mSubObj.mStatus == Status.ChargeFail)
			{
				mMTType = MTType.BuySugNotExtend;
				return AddToList();
			}

			// Phiên chơi chưa bắt đầu
			if (mCal_Current.before(mCal_Begin) || mCal_Current.after(mCal_End)
					|| CurrentData.Get_Current_QuestionObj().IsNull()
					|| CurrentData.Get_Current_SuggestObj().size() < 1)
			{
				mMTType = MTType.BuySugExpire;
				return AddToList();
			}

			// Kiểm tra mua vượt quá giới hạn
			if (mSubObj.CheckLastSuggestDate(mCal_Current) && mSubObj.SuggestByDay >= 20)
			{
				mMTType = MTType.BuySugLimit;
				return AddToList();
			}

			// Đã trả lời đúng trước đó thì không được chơi nữa
			if (mSubObj.CheckLastAnswerDate(mCal_Current) && mSubObj.mLastAnswerStatus == Play.Status.CorrectAnswer)
			{
				mMTType = MTType.BuySugAnswerRight;
				return AddToList();
			}
			
			if (mSubObj.CheckLastSuggestDate(mCal_Current))
			{
				mSubObj.SuggestByDay++;
			}
			else
			{
				mSubObj.SuggestByDay = 1;
			}
			
			mSuggestObj = CurrentData.Get_SuggestObj(mSubObj.SuggestByDay);
			if (mSuggestObj.IsNull())
			{
				mLog.log.warn("Du kien khong lay duoc, kiem tra ngay");
				mMTType = MTType.BuySugFail;
				return AddToList();
			}
			
			ErrorCode mResult = mCharge.ChargeBuyContent(mSubObj, ChannelType.FromInt(mMsgObject.getChannelType()),
					"CONTENT");

			// Mua dữ kiện không đủ tiền
			if (mResult == ErrorCode.BlanceTooLow)
			{
				mMTType = MTType.BuySugNotEnoughMoney;
				return AddToList();
			}

			// Trừ tiền không thành công
			if (mResult != ErrorCode.ChargeSuccess)
			{
				mMTType = MTType.BuySugFail;
				return AddToList();
			}

			CreateUpdateSub();

			// Cập nhật thông tin vào DB
			UpdateSubInfo();

			Insert_Play();

			Update_SuggestCount();

			mMTType = MTType.BuySugSuccess;
			return AddToList();
		}
		catch (Exception ex)
		{
			mLog.log.error(Common.GetStringLog(msgObject), ex);
			mMTType = MTType.BuySugFail;
			return AddToList();
		}
		finally
		{
			// Insert vao log
			Insert_MOLog();
			mLog.log.debug(Common.GetStringLog(mMsgObject));
		}
	}
}

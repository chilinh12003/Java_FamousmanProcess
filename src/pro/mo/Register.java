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
import uti.utility.VNPApplication;
import uti.utility.MyConvert;
import uti.utility.MyLogger;
import dat.content.DefineMT;
import dat.content.DefineMT.MTType;
import dat.history.MOLog;
import dat.history.MOObject;
import dat.sub.Subscriber;
import dat.sub.Subscriber.Status;
import dat.sub.SubscriberObject;
import dat.sub.SubscriberObject.InitType;
import dat.sub.UnSubscriber;
import db.define.MyTableModel;

public class Register extends ContentAbstract
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());
	Collection<MsgObject> ListMessOject = new ArrayList<MsgObject>();

	MsgObject mMsgObject = null;
	SubscriberObject mSubObj = new SubscriberObject();

	Calendar mCal_Current = Calendar.getInstance();
	Calendar mCal_Expire = Calendar.getInstance();

	Subscriber mSub = null;
	UnSubscriber mUnSub = null;
	MOLog mMOLog = null;
	dat.content.Keyword mKeyword = null;

	MyTableModel mTable_MOLog = null;
	MyTableModel mTable_Sub = null;

	pro.charge.Charge mCharge = new Charge();
	DefineMT.MTType mMTType = MTType.RegFail;

	String MTContent = "";

	/**
	 * ID của đối tác, khi đăng ký qua các kênh của đối tác
	 */
	Integer PartnerID = 0;

	// Thời gian miễn phí để chèn vào MT trả về cho khách hàng
	String FreeTime = "ngay dau tien";

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

			mCal_Expire.set(Calendar.MILLISECOND, 0);
			mCal_Expire.set(mCal_Current.get(Calendar.YEAR), mCal_Current.get(Calendar.MONTH),
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
			MTContent = Common.GetDefineMT_Message(mMTType, FreeTime);

			if (!MTContent.equalsIgnoreCase(""))
			{
				mMsgObject.setUsertext(MTContent);
				mMsgObject.setContenttype(LocalConfig.LONG_MESSAGE_CONTENT_TYPE);
				mMsgObject.setMsgtype(1);
				ListMessOject.add(new MsgObject((MsgObject) mMsgObject.clone()));
				AddToMOLog(mMTType, MTContent);
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
					Calendar.getInstance().getTime(), new VNPApplication(), null, null, PartnerID);

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

	private boolean Insert_Sub() throws Exception
	{
		try
		{
			mTable_Sub.Clear();
			mTable_Sub = mSubObj.AddNewRow(mTable_Sub);

			if (!mSub.Insert(0, mTable_Sub.GetXML()))
			{
				mLog.log.info("Insert vao table Subscriber KHONG THANH CONG: XML Insert-->" + mTable_Sub.GetXML());
				return false;
			}

			return true;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	private boolean MoveToUnSub() throws Exception
	{
		try
		{
			mTable_Sub.Clear();
			mTable_Sub = mSubObj.AddNewRow(mTable_Sub);

			if (!mSub.Move(0, mTable_Sub.GetXML()))
			{
				mLog.log.info("Move tu UnSub Sang Sub KHONG THANH CONG: XML Insert-->" + mTable_Sub.GetXML());
				return false;
			}

			return true;
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	protected void CreateSub(SubscriberObject.InitType mInitType) throws Exception
	{

		switch (mInitType)
		{
			case NewReg :
				mSubObj = new SubscriberObject();
				mSubObj.MSISDN = mMsgObject.getUserid();
				mSubObj.FirstDate = mCal_Current.getTime();
				mSubObj.ResetDate = mCal_Current.getTime();
				mSubObj.EffectiveDate = mCal_Current.getTime();
				mSubObj.ExpiryDate = mCal_Expire.getTime();

				// mSubObj.RetryChargeDate=
				// mSubObj.RetryChargeCount=
				// mSubObj.RenewChargeDate=
				mSubObj.mChannelType = ChannelType.FromInt(mMsgObject.getChannelType());
				mSubObj.mStatus = Status.ActiveFree;
				mSubObj.PID = MyConvert.GetPIDByMSISDN(mMsgObject.getUserid(), LocalConfig.MAX_PID);
				// mSubObj.LastSuggestrID=
				// mSubObj.SuggestByDay=
				// mSubObj.TotalSuggest=
				/*
				 * mSubObj.LastSuggestDate= mSubObj.AnswerForSuggestID=
				 * mSubObj.LastAnswer= mSubObj.AnswerStatusID=
				 * mSubObj.AnswerByDay= mSubObj.LastAnswerDate=
				 * mSubObj.DeregDate=
				 */
				mSubObj.PartnerID = PartnerID;
				mSubObj. mVNPApp =new VNPApplication();

				break;
			case RegAgain :
				// mSubObj = new SubscriberObject();
				// mSubObj.MSISDN=mMsgObject.getUserid();
				// mSubObj.FirstDate= mCal_Current.getTime();
				if (mSubObj.IsFreeReg(90))
				{
					mSubObj.ResetDate = mCal_Current.getTime();
				}
				mSubObj.EffectiveDate = mCal_Current.getTime();
				mSubObj.ExpiryDate = mCal_Expire.getTime();

				// mSubObj.RetryChargeDate=
				// mSubObj.RetryChargeCount=
				// mSubObj.RenewChargeDate=
				mSubObj.mChannelType = ChannelType.FromInt(mMsgObject.getChannelType());
				mSubObj.mStatus = Status.Active;
				mSubObj.PID = MyConvert.GetPIDByMSISDN(mMsgObject.getUserid(), LocalConfig.MAX_PID);
				mSubObj.LastSuggestrID = 0;
				mSubObj.SuggestByDay = 0;
				mSubObj.TotalSuggest = 0;
				mSubObj.LastSuggestDate = null;
				mSubObj.AnswerForSuggestID = 0;
				mSubObj.LastAnswer = "";
				mSubObj.mLastAnswerStatus = dat.history.Play.Status.Nothing;
				mSubObj.AnswerByDay = 0;
				mSubObj.LastAnswerDate = null;
				// mSubObj.DeregDate=

				mSubObj.PartnerID = PartnerID;
				mSubObj. mVNPApp =new VNPApplication();
				mSubObj.UserName = "";
				mSubObj.IP = "";
				break;
			case UndoReg :
				// mSubObj = new SubscriberObject();
				// mSubObj.MSISDN=mMsgObject.getUserid();
				// mSubObj.FirstDate= mCal_Current.getTime();
				mSubObj.ResetDate = mCal_Current.getTime();
				mSubObj.EffectiveDate = mCal_Current.getTime();
				mSubObj.ExpiryDate = mCal_Expire.getTime();

				// mSubObj.RetryChargeDate=
				// mSubObj.RetryChargeCount=
				// mSubObj.RenewChargeDate=
				mSubObj.mChannelType = ChannelType.FromInt(mMsgObject.getChannelType());
				mSubObj.mStatus = Status.ActiveFree;
				mSubObj.LastSuggestrID = 0;
				mSubObj.SuggestByDay = 0;
				mSubObj.TotalSuggest = 0;
				mSubObj.LastSuggestDate = null;
				mSubObj.AnswerForSuggestID = 0;
				mSubObj.LastAnswer = "";
				mSubObj.mLastAnswerStatus = dat.history.Play.Status.Nothing;
				mSubObj.AnswerByDay = 0;
				mSubObj.LastAnswerDate = null;
				// mSubObj.DeregDate=

				mSubObj.PartnerID = PartnerID;
				mSubObj. mVNPApp =new VNPApplication();
				mSubObj.UserName = "";
				mSubObj.IP = "";
				break;
		}
	}

	protected Collection<MsgObject> getMessages(MsgObject msgObject, Keyword keyword) throws Exception
	{
		try
		{
			// Khoi tao
			Init(msgObject, keyword);

			// Lấy đối tác dựa vào Keyword.
			PartnerID = mKeyword.GetPartnerID(msgObject.getUsertext());

			Integer PID = MyConvert.GetPIDByMSISDN(mMsgObject.getUserid(), LocalConfig.MAX_PID);

			// Lấy thông tin khách hàng đã đăng ký
			MyTableModel mTable_Sub = mSub.Select(2, PID.toString(), mMsgObject.getUserid());

			mSubObj = SubscriberObject.Convert(mTable_Sub, false);

			if (mSubObj.IsNull())
			{
				mTable_Sub = mUnSub.Select(2, PID.toString(), mMsgObject.getUserid());

				if (mTable_Sub.GetRowCount() > 0) mSubObj = SubscriberObject.Convert(mTable_Sub, true);
			}

			mSubObj.PID = MyConvert.GetPIDByMSISDN(mMsgObject.getUserid(), LocalConfig.MAX_PID);

			// Đăng ký mới (chưa từng đăng ký trước đây)
			if (mSubObj.IsNull())
			{
				// Tạo dữ liệu cho đăng ký mới
				CreateSub(InitType.NewReg);

				ErrorCode mResult = mCharge.ChargeRegFree(mSubObj, ChannelType.FromInt(mMsgObject.getChannelType()),
						"DK Free");

				if (mResult != ErrorCode.ChargeSuccess)
				{
					mMTType = MTType.RegFail;
					return AddToList();
				}

				if (Insert_Sub())
				{
					mMTType = MTType.RegNewSuccess;
				}
				else
				{
					mMTType = MTType.RegFail;
				}

				return AddToList();
			}

			// Nếu đã đăng ký rồi và tiếp tục đăng ký
			if (!mSubObj.IsNull() && mSubObj.IsDereg == false)
			{
				// Kiểm tra còn free hay không
				if (mSubObj.mStatus == Status.ActiveFree || mSubObj.mStatus == Status.ActiveTrial
						|| mSubObj.mStatus == Status.ActiveBundle || mSubObj.mStatus == Status.ActivePromotion)
				{
					mMTType = MTType.RegRepeatFree;
					return AddToList();
				}
				else
				{
					mMTType = MTType.RegRepeatNotFree;
					return AddToList();
				}
			}

			// Nếu trước đó số điện thoại đã được Hủy thuê bao hoặc
			// đã sử dụng dịch vụ được 90 ngày
			if (mSubObj.IsDereg && (mSubObj.mStatus == dat.sub.Subscriber.Status.UndoSub || !mSubObj.IsFreeReg(90)))
			{
				CreateSub(InitType.UndoReg);
				ErrorCode mResult = mCharge.ChargeRegFree(mSubObj, ChannelType.FromInt(mMsgObject.getChannelType()),
						"DK Free");
				if (mResult != ErrorCode.ChargeSuccess)
				{
					mMTType = MTType.RegFail;
					return AddToList();
				}

				if (MoveToUnSub())
				{
					mMTType = MTType.RegNewSuccess;
				}
				else
				{
					mMTType = MTType.RegFail;
				}

				return AddToList();
			}
			// Đã đăng ký trước đó nhưng đang hủy
			if (mSubObj.IsDereg)
			{
				CreateSub(InitType.RegAgain);

				// đồng bộ thuê bao sang Vinpahone
				ErrorCode mResult = mCharge.ChargeReg(mSubObj, ChannelType.FromInt(mMsgObject.getChannelType()),
						"DK Pay");

				// Charge
				if (mResult == ErrorCode.BlanceTooLow)
				{
					mMTType = MTType.RegNotEnoughMoney;
					return AddToList();
				}
				if (mResult != ErrorCode.ChargeSuccess)
				{
					mMTType = MTType.RegFail; // Đăng ký lại nhưng mất tiền
					return AddToList();
				}

				// Nếu xóa unsub hoặc Insert sub không thành công thì thông
				// báo lỗi
				if (MoveToUnSub())
				{
					mMTType = MTType.RegAgainSuccessNotFree;
					return AddToList();
				}

				mMTType = MTType.RegFail; // Đăng ký lại nhưng mất tiền
				return AddToList();
			}

			mMTType = MTType.RegFail;
			return AddToList();
		}
		catch (Exception ex)
		{
			mLog.log.error(Common.GetStringLog(msgObject), ex);
			mMTType = MTType.RegFail;
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

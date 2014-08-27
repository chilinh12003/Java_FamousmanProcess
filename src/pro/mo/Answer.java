package pro.mo;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;

import pro.charge.Charge;
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
import uti.utility.MyText;
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
import dat.sub.SubscriberObject;
import dat.sub.UnSubscriber;
import db.define.MyTableModel;

public class Answer extends ContentAbstract
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());
	Collection<MsgObject> ListMessOject = new ArrayList<MsgObject>();

	MsgObject mMsgObject = null;
	SubscriberObject mSubObj = new SubscriberObject();

	SuggestObject mSuggestObj = new SuggestObject();

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

	String UserAnswer = "";

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

			String MTContent = Common.GetDefineMT_Message(mMTType);
			MTContent = MTContent.replace("[Prize]", CurrentData.Get_Current_QuestionObj().Prize);
			MTContent = MTContent.replace("[PlayDate]", CurrentData.Get_Current_QuestionObj().Get_PlayDate());
			MTContent = MTContent.replace("[NextDate]", CurrentData.Get_Current_QuestionObj().Get_NextDate());
			
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
		if (mSubObj.CheckLastAnswerDate(mCal_Current))
		{
			mSubObj.AnswerByDay++;
		}
		else
		{
			mSubObj.AnswerByDay = 1;
		}

		if (UserAnswer.equalsIgnoreCase(CurrentData.Get_Current_QuestionObj().RightAnswer))
		{
			mSubObj.mLastAnswerStatus = Play.Status.CorrectAnswer;
		}
		else
		{
			mSubObj.mLastAnswerStatus = Play.Status.IncorrectAnswer;
		}
		mSuggestObj = CurrentData.Get_SuggestObj_BuyID(mSubObj.LastSuggestrID);

		mSubObj.LastAnswerDate = mCal_Current.getTime();
		mSubObj.AnswerForSuggestID = mSubObj.LastSuggestrID;
		mSubObj.LastAnswer = UserAnswer;
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
			mObject.mPlayType = PlayType.Answer;
			mObject.MSISDN = mSubObj.MSISDN;
			mObject.mStatus = mSubObj.mLastAnswerStatus;
			mObject.OrderNumber = mSuggestObj.OrderNumber;
			mObject.PID = mSubObj.PID;
			mObject.QuestionID = mSuggestObj.QuestionID;
			mObject.ReceiveDate = mMsgObject.getReceiveDate();
			mObject.SuggestID = mSuggestObj.SuggestID;
			mObject.UserAnswer = UserAnswer;

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
			SuggestCountObject mObject = CurrentData.Get_SuggestCountObj(mSuggestObj);
			if (mSubObj.mLastAnswerStatus == Play.Status.CorrectAnswer)
			{
				mObject.CorrectCount++;
			}
			else
			{
				mObject.IncorrectCount++;
			}
			mObject.LastUpdate = mCal_Current.getTime();

			MyTableModel mTable = CurrentData.GetTable_SuggestCount();

			mTable = mObject.AddNewRow(mTable);

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

			if (!mSub.Update(2, mTable_Sub.GetXML()))
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

	private String Get_UserAnswer() throws Exception
	{
		String MO = mMsgObject.getMO().substring(mMsgObject.getKeyword().length());
		MO = MyText.RemoveSpecialLetter(2, MO);

		return MO;
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
				mMTType = MTType.AnswerNotReg;
				return AddToList();
			}

			// Phiên chơi chưa bắt đầu
			if (mCal_Current.before(mCal_Begin) || mCal_Current.after(mCal_End)
					|| CurrentData.Get_Current_QuestionObj().IsNull())
			{
				mMTType = MTType.AnswerExpire;
				return AddToList();
			}

			// Chưa mua mà đã trả lời.
			if (!mSubObj.CheckLastSuggestDate(mCal_Current) || mSubObj.LastSuggestrID == 0)
			{
				mMTType = MTType.AnswerNotBuy;
				return AddToList();
			}

			// Mỗi 1 lần mua chỉ được trả lời 1 lần
			if (mSubObj.CheckLastSuggestDate(mCal_Current) && mSubObj.CheckLastAnswerDate(mCal_Current)
					&& mSubObj.AnswerForSuggestID == mSubObj.LastSuggestrID)
			{
				mMTType = MTType.AnswerLimit;
				return AddToList();
			}

			// Kiểm tra mua vượt quá giới hạn
			if (mSubObj.CheckLastAnswerDate(mCal_Current) && mSubObj.AnswerByDay >= 20)
			{
				mMTType = MTType.AnswerLimit;
				return AddToList();
			}

			UserAnswer = Get_UserAnswer();

			CreateUpdateSub();

			// Cập nhật thông tin vào DB
			UpdateSubInfo();

			Insert_Play();

			Update_SuggestCount();

			if (mSubObj.mLastAnswerStatus == Play.Status.CorrectAnswer)
			{
				mMTType = MTType.AnswerSuccess;
				return AddToList();
			}
			else
			{
				mMTType = MTType.AnswerWrong;
				return AddToList();
			}
		}
		catch (Exception ex)
		{
			mLog.log.error(Common.GetStringLog(msgObject), ex);
			mMTType = MTType.AnswerFail;
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
package pro.check;

import java.util.Calendar;
import java.util.Vector;

import pro.server.Common;
import pro.server.CurrentData;
import pro.server.LocalConfig;
import pro.server.Program;
import uti.utility.MyCheck;
import uti.utility.MyConfig;
import uti.utility.MyLogger;
import dat.content.DefineMT.MTType;
import dat.content.News;
import dat.content.News.NewsType;
import dat.content.News.Status;
import dat.content.NewsObject;
import dat.content.QuestionObject;
import dat.history.Play;
import dat.history.PlayObject;
import dat.history.SuggestCount;
import dat.history.SuggestCountLog;
import dat.history.SuggestCountObject;
import dat.history.Winner;
import db.define.MyDataRow;
import db.define.MyTableModel;

public class CheckFinish extends Thread
{
	boolean IsRunning = false;
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());

	public CheckFinish()
	{

	}

	public void run()
	{
		while (Program.processData)
		{
			mLog.log.debug("---------------BAT DAU CHECK FINISH PHIEN --------------------");
			try
			{
				boolean IsSameTime = false;
				for (String CheckTime : LocalConfig.FINISH_LIST_TIME)
				{
					Calendar mCal_Current = Calendar.getInstance();
					if (mCal_Current.get(Calendar.HOUR_OF_DAY) != Integer.parseInt(CheckTime))
					{
						continue;
					}
					IsSameTime = true;
				}

				if (IsSameTime && IsRunning == false)
				{
					QuestionObject mQuestionObj = new QuestionObject();

					mQuestionObj = CurrentData.Get_Yesterday_QuestionObj();

					if (mQuestionObj.IsNull())
					{
						mLog.log.warn("Khong tim thay Question ngay hom qua -->" + mQuestionObj.GetLog());
						mLog.log.warn("KET THUC PROCESS TIM NGUOI CHIEN THANG -- DO KO LAY DUOC QUESTION CUA NGAY HOM QUA");
						Sleep();
						continue;
					}
					else
					{
						mLog.log.info("Thong tin ve Question -->" + mQuestionObj.GetLog());
					}

					// Lấy danh sách các Dữ kiện đã được mua ít nhất và tồn tài
					// câu trả lời đúng
					Vector<SuggestCountObject> mListSuggestCount_Min = GetListSuggestCount_BuyMin(mQuestionObj.QuestionID);

					// Lấy người chiến thắng đã trả lời đúng, sớm nhất trong tập
					// mListSuggestCount_Min ở trên.
					PlayObject mPlayObj_Winner = GetWinner(mListSuggestCount_Min);

					mLog.log.info("Thong tin nguoi chien thang -->" + mPlayObj_Winner.GetLog());

					if (InsertWinner(mListSuggestCount_Min, mPlayObj_Winner, mQuestionObj))
					{
						// Insert MT thông báo người chiến thằng vào table news,
						// chờ admin duyệt để push tin
						InsertNotifyWinner(mPlayObj_Winner, mQuestionObj);
					}
					else
					{
						mLog.log.warn("Khong the insert xuong table winner");
					}

					// Chuyển 2 table Play và SuggestCount sang Table Log
					for (int j = 0; j < LocalConfig.FINISH_PROCESS_NUMBER; j++)
					{
						MovePlay mMovePlay = new MovePlay();
						mMovePlay.ProcessIndex = j;
						mMovePlay.ProcessNumber = LocalConfig.FINISH_PROCESS_NUMBER;
						mMovePlay.RowCount = LocalConfig.FINISH_ROWCOUNT;
						mMovePlay.StartDate = Calendar.getInstance().getTime();
						mMovePlay.setPriority(Thread.MAX_PRIORITY);
						mMovePlay.start();
						Thread.sleep(500);
					}

					// Xoa du lieu SuggestCount
					Delete_SuggestCount();
				}
			}
			catch (Exception ex)
			{
				mLog.log.error(ex);
			}

			IsRunning = false;

			Sleep();
		}
	}

	private void Sleep()
	{
		try
		{
			mLog.log.debug("CHECK FINISH SE DELAY " + LocalConfig.FINISH_TIME_DELAY + " {HUT.");
			mLog.log.debug("---------------KET THUC CHECK FINISH --------------------");
			sleep(LocalConfig.FINISH_TIME_DELAY * 60 * 1000);
		}
		catch (InterruptedException ex)
		{
			mLog.log.error("Error Sleep thread", ex);
		}
	}
	private void Delete_SuggestCount() throws Exception
	{
		MyTableModel mTable = new MyTableModel(null, null);
		try
		{
			SuggestCount mSuggestCount = new SuggestCount(LocalConfig.mDBConfig_MSSQL);
			SuggestCountLog mSuggestCountLog = new SuggestCountLog(LocalConfig.mDBConfig_MSSQL);

			mTable = mSuggestCount.Select(4);

			if (mSuggestCountLog.Insert(0, mTable.GetXML()))
			{
				mSuggestCount.Delete(0, mTable.GetXML());
			}

			mLog.log.debug("Xu ly xong tong so row SuggestCount:" + mTable.GetRowCount().toString());

			return;
		}
		catch (Exception ex)
		{
			mLog.log.debug("Loi trong di chuyen du lieu SuggestCount --> SuggestCountLog", ex);
			throw ex;
		}
		finally
		{
			mLog.log.debug("Ket thuc di chuyen SuggestCount --> SuggestCountLog");
		}
	}

	/**
	 * Insert xuống table news, chờ admin duyệt để push tin
	 * 
	 * @param mPlayObj_Winner
	 * @param mQuestionObj
	 * @return
	 * @throws Exception
	 */
	private boolean InsertNotifyWinner(PlayObject mPlayObj_Winner, QuestionObject mQuestionObj) throws Exception
	{

		if (mPlayObj_Winner.IsNull())
			return true;

		String MT = Common.GetDefineMT_Message(MTType.NotifyWinner);
		String MSISDN = MyCheck.ValidPhoneNumber(mPlayObj_Winner.MSISDN, "0");
		MSISDN = MSISDN.substring(0, MSISDN.length() - 2) + "xx";
		MT = MT.replace("[Winner]", MSISDN);
		MT = MT.replace("[RightAnswer]", mQuestionObj.RightAnswer);
		MT = MT.replace(" [PlayDate]", mQuestionObj.Get_PlayDate());
		MT = MT.replace("[Prize]", mQuestionObj.Prize);
		MT = MT.replace("[Price]", mQuestionObj.Price);

		NewsObject mNewsObj = new NewsObject();
		mNewsObj.NewsName = "Push Tin Nguoi Chien thang ngay: " + mQuestionObj.Get_PlayDate();
		mNewsObj.mNewsType = NewsType.Winner;
		mNewsObj.mStatus = Status.Waiting;
		mNewsObj.MT = MT;

		Calendar mCal_PushTime = Calendar.getInstance();
		mCal_PushTime.set(Calendar.HOUR_OF_DAY, 14);
		mCal_PushTime.set(Calendar.MINUTE, 15);
		mCal_PushTime.set(Calendar.MILLISECOND, 0);

		mNewsObj.PushTime = mCal_PushTime.getTime();
		mNewsObj.QuestionID = mQuestionObj.QuestionID;
		mNewsObj.CreateDate = Calendar.getInstance().getTime();
		mNewsObj.Priority = 0;

		News mNews = new News(LocalConfig.mDBConfig_MSSQL);

		MyTableModel mTable_News = mNews.Select(0);
		mTable_News = mNewsObj.AddNewRow(mTable_News);

		return mNews.Insert(0, mTable_News.GetXML());
	}

	private boolean InsertWinner(Vector<SuggestCountObject> mListSuggestCount_Min, PlayObject mPlayObj_Winner,
			QuestionObject mQuestionObj) throws Exception
	{
		if (mPlayObj_Winner.IsNull())
			return true;

		Winner mWinner = new Winner(LocalConfig.mDBConfig_MSSQL);
		MyTableModel mTable_Winner = mWinner.Select(0);

		MyDataRow mRow = mTable_Winner.CreateNewRow();

		// nếu không có người nào chiến thắng
		if (mListSuggestCount_Min == null || mListSuggestCount_Min.size() < 1 || mPlayObj_Winner.IsNull())
		{
			mLog.log.warn("Khong tim thay nguoi chien thang");
			mRow.SetValueCell("QuestionID", mQuestionObj.QuestionID);
			mRow.SetValueCell("PlayDate", MyConfig.Get_DateFormat_InsertDB().format(mQuestionObj.PlayDate));
		}
		else
		{
			SuggestCountObject mSuggestCountObj_Winner = new SuggestCountObject();
			for (SuggestCountObject mObject : mListSuggestCount_Min)
			{
				if (mObject.SuggestID == mPlayObj_Winner.SuggestID)
				{
					mSuggestCountObj_Winner = (SuggestCountObject) mObject.clone();
					break;
				}
			}
			mRow.SetValueCell("QuestionID", mPlayObj_Winner.QuestionID);
			mRow.SetValueCell("PlayDate", MyConfig.Get_DateFormat_InsertDB().format(mSuggestCountObj_Winner.PlayDate));
			mRow.SetValueCell("MSISDN", mPlayObj_Winner.MSISDN);
			mRow.SetValueCell("SuggestID", mPlayObj_Winner.SuggestID);
			mRow.SetValueCell("RightAnswer", mQuestionObj.RightAnswer);
			mRow.SetValueCell("UserAnswer", mPlayObj_Winner.UserAnswer);
			mRow.SetValueCell("BuyCount", mSuggestCountObj_Winner.BuyCount);
			mRow.SetValueCell("CorrectCount", mSuggestCountObj_Winner.CorrectCount);
			mRow.SetValueCell("IncorrectCount", mSuggestCountObj_Winner.IncorrectCount);
			mRow.SetValueCell("ReceiveDate", MyConfig.Get_DateFormat_InsertDB().format(mPlayObj_Winner.ReceiveDate));
		}

		mTable_Winner.AddNewRow(mRow);
		boolean Result = mWinner.Insert(0, mTable_Winner.GetXML());
		return Result;

	}

	/**
	 * Lấy danh sách các Dữ kiện tồn tại câu trả lời đúng và có số lần mua thập
	 * nhất
	 * 
	 * @return
	 * @throws Exception
	 */
	private Vector<SuggestCountObject> GetListSuggestCount_BuyMin(int QuestionID) throws Exception
	{
		SuggestCount mSuggestCount = new SuggestCount(LocalConfig.mDBConfig_MSSQL);

		// Lấy tất cả các thống kê
		MyTableModel mTable = mSuggestCount.Select(2, Integer.toString(QuestionID));

		Vector<SuggestCountObject> mListSuggestCount = SuggestCountObject.ConvertToList(mTable);

		// Số lượng mua dữ kiện ít nhất nhưng phải tồn tại câu trả
		// lời đúng
		int MinBuyCount = 1000000;

		// Tìm giá trị nhỏ nhất của Số lần mua dữ kiện
		for (SuggestCountObject mObject : mListSuggestCount)
		{
			if (mObject.CorrectCount > 0 && mObject.BuyCount <= MinBuyCount)
			{
				MinBuyCount = mObject.BuyCount;
			}
		}

		// Lấy các SuggestCount có số lần mua = MinBuyCount
		Vector<SuggestCountObject> mListSuggestCount_Min = new Vector<SuggestCountObject>();

		for (SuggestCountObject mObject : mListSuggestCount)
		{
			if (mObject.CorrectCount > 0 && mObject.BuyCount == MinBuyCount)
			{
				mListSuggestCount_Min.add((SuggestCountObject) mObject.clone());
			}
		}

		return mListSuggestCount_Min;
	}

	/**
	 * Lấy người chiến thằng
	 * 
	 * @param mListSuggestCount_Min
	 * @return
	 * @throws Exception
	 */
	private PlayObject GetWinner(Vector<SuggestCountObject> mListSuggestCount_Min) throws Exception
	{
		// Tìm tất cả các thuê bao đã trả lời đúng dữ kiện này.
		Vector<PlayObject> mListPlayObj = new Vector<PlayObject>();
		Play mPlay = new Play(LocalConfig.mDBConfig_MSSQL);

		// Lấy danh sách KH đã trả lời đúng và ít nhất
		for (SuggestCountObject mObject : mListSuggestCount_Min)
		{
			long MaxLogID = 0;
			MyTableModel mTable_Play = mPlay.Select(4, "10", Play.PlayType.Answer.GetValue().toString(),
					Integer.toString(Play.Status.CorrectAnswer.GetValue()), Integer.toString(mObject.SuggestID),
					Long.toString(MaxLogID), "1", "0");

			while (Program.processData && mTable_Play != null && mTable_Play.GetRowCount() > 0)
			{
				Vector<PlayObject> mListPlayObj_Temp = PlayObject.ConvertToList(mTable_Play);
				for (PlayObject mPlayObj : mListPlayObj_Temp)
				{
					MaxLogID = mPlayObj.LogID;
					mListPlayObj.add((PlayObject) mPlayObj.clone());
				}

				mTable_Play = mPlay.Select(4, "10", Play.PlayType.Answer.GetValue().toString(),
						Integer.toString(Play.Status.CorrectAnswer.GetValue()), Integer.toString(mObject.SuggestID),
						Long.toString(MaxLogID), "1", "0");
			}
		}

		// Tìm người trả lời sớm nhất
		PlayObject mPlayObj_Winner = new PlayObject();
		Calendar mCal_Oldest = Calendar.getInstance();
		Calendar mCal_AnswerDate = Calendar.getInstance();
		for (PlayObject mPlayObj : mListPlayObj)
		{
			mCal_AnswerDate.setTime(mPlayObj.ReceiveDate);
			if (mCal_AnswerDate.before(mCal_Oldest))
			{
				mCal_Oldest.setTime(mCal_AnswerDate.getTime());
				mPlayObj_Winner = (PlayObject) mPlayObj.clone();
			}
		}
		return mPlayObj_Winner;
	}

}

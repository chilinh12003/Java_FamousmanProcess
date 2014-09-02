package pro.server;

import java.util.Calendar;
import java.util.Vector;

import uti.utility.MyConfig;
import dat.content.Question;
import dat.content.QuestionObject;
import dat.content.Suggest;
import dat.content.SuggestObject;
import dat.history.ChargeLog;
import dat.history.MOLog;
import dat.history.Play;
import dat.history.SuggestCount;
import dat.history.SuggestCountObject;
import dat.sub.Subscriber;
import dat.sub.UnSubscriber;
import db.define.MyTableModel;

/**
 * Chứa các đối tượng đã table mẫu ở Database Các đối tượng này sẽ được load khi
 * chương trình bắt đầu chạy
 * 
 * @author Administrator
 * 
 */
public class CurrentData
{
	public static MyTableModel mTable_MOLog = null;

	public static synchronized MyTableModel GetTable_MOLog() throws Exception
	{
		/*
		 * if (mTable_MOLog == null) {
		 */
		MOLog mMOLog = new MOLog(LocalConfig.mDBConfig_MSSQL);
		mTable_MOLog = mMOLog.Select(0);
		return (MyTableModel) mTable_MOLog.clone();
		/*
		 * } else { MyTableModel mTable = ((MyTableModel) mTable_MOLog.clone());
		 * mTable.Clear(); return mTable; }
		 */
	}

	public static MyTableModel mTable_Sub = null;

	public static synchronized MyTableModel GetTable_Sub() throws Exception
	{
		// if (mTable_Sub == null)
		// {
		Subscriber mSub = new Subscriber(LocalConfig.mDBConfig_MSSQL);
		mTable_Sub = mSub.Select(0);
		return (MyTableModel) mTable_Sub.clone();
		// }
		// else
		// {
		// MyTableModel mTable = ((MyTableModel) mTable_Sub.clone());
		// mTable.Clear();
		// return mTable;
		// }
	}

	public static MyTableModel mTable_UnSub = null;

	public static synchronized MyTableModel GetTable_UnSub() throws Exception
	{
		/*
		 * if (mTable_UnSub == null) {
		 */
		UnSubscriber mUnSub = new UnSubscriber(LocalConfig.mDBConfig_MSSQL);
		mTable_UnSub = mUnSub.Select(0);
		return (MyTableModel) mTable_UnSub.clone();
		/*
		 * } else { MyTableModel mTable = ((MyTableModel) mTable_UnSub.clone());
		 * mTable.Clear(); return mTable; }
		 */
	}

	public static MyTableModel mTable_ChargeLog = null;

	public static synchronized MyTableModel GetTable_ChargeLog() throws Exception
	{
		/*
		 * if (mTable_ChargeLog == null) {
		 */
		ChargeLog mChargeLog = new ChargeLog(LocalConfig.mDBConfig_MSSQL);
		mTable_ChargeLog = mChargeLog.Select(0);
		return (MyTableModel) mTable_ChargeLog.clone();
		/*
		 * } else { MyTableModel mTable = ((MyTableModel)
		 * mTable_ChargeLog.clone()); mTable.Clear(); return mTable; }
		 */
	}

	public static MyTableModel mTable_Play = null;

	public static synchronized MyTableModel GetTable_Play() throws Exception
	{
		/*
		 * if (mTable_Play == null) {
		 */
		Play mPlay = new Play(LocalConfig.mDBConfig_MSSQL);
		mTable_Play = mPlay.Select(0);
		return (MyTableModel) mTable_Play.clone();
		/*
		 * } else { MyTableModel mTable = ((MyTableModel) mTable_Play.clone());
		 * mTable.Clear(); return mTable; }
		 */
	}

	public static MyTableModel mTable_SuggestCount = null;

	public static synchronized MyTableModel GetTable_SuggestCount() throws Exception
	{
		/*
		 * if (mTable_SuggestCount == null) {
		 */
		SuggestCount mSuggestCount = new SuggestCount(LocalConfig.mDBConfig_MSSQL);
		mTable_SuggestCount = mSuggestCount.Select(0);
		return (MyTableModel) mTable_SuggestCount.clone();
		/*
		 * } else { MyTableModel mTable = ((MyTableModel)
		 * mTable_SuggestCount.clone()); mTable.Clear(); return mTable; }
		 */
	}

	private static QuestionObject Current_QuestionObj = new QuestionObject();

	/**
	 * Lấy câu hỏi theo ngày
	 * 
	 * @param mCal_Current
	 * @return
	 * @throws Exception
	 */
	public static synchronized QuestionObject Get_Current_QuestionObj() throws Exception
	{
		Calendar mCal_Current = Calendar.getInstance();
		if (Current_QuestionObj != null && Current_QuestionObj.CheckPlayDate(mCal_Current))
			return Current_QuestionObj;

		Calendar mCal_PlayDate = Calendar.getInstance();

		mCal_PlayDate.set(Calendar.MILLISECOND, 0);
		mCal_PlayDate.set(mCal_Current.get(Calendar.YEAR), mCal_Current.get(Calendar.MONTH),
				mCal_Current.get(Calendar.DATE), 0, 0, 0);

		Question mQuestion = new Question(LocalConfig.mDBConfig_MSSQL);
		MyTableModel mTable = mQuestion.Select(2, MyConfig.Get_DateFormat_InsertDB().format(mCal_PlayDate.getTime()));
		Current_QuestionObj = QuestionObject.Convert(mTable);

		return Current_QuestionObj;
	}

	/**
	 * Lấy câu hỏi của ngày hôm qua
	 * 
	 * @return
	 * @throws Exception
	 */
	public static synchronized QuestionObject Get_Yesterday_QuestionObj() throws Exception
	{
		Calendar mCal_Yesterday = Calendar.getInstance();
		mCal_Yesterday.add(Calendar.DATE, -1);
		Calendar mCal_PlayDate = Calendar.getInstance();

		mCal_PlayDate.set(Calendar.MILLISECOND, 0);
		mCal_PlayDate.set(mCal_Yesterday.get(Calendar.YEAR), mCal_Yesterday.get(Calendar.MONTH),
				mCal_Yesterday.get(Calendar.DATE), 0, 0, 0);

		Question mQuestion = new Question(LocalConfig.mDBConfig_MSSQL);
		MyTableModel mTable = mQuestion.Select(2, MyConfig.Get_DateFormat_InsertDB().format(mCal_PlayDate.getTime()));

		return QuestionObject.Convert(mTable);
	}

	public static synchronized QuestionObject Get_QuestionObj(int QuestionID) throws Exception
	{

		Question mQuestion = new Question(LocalConfig.mDBConfig_MSSQL);
		MyTableModel mTable = mQuestion.Select(1, Integer.toString(QuestionID));

		return QuestionObject.Convert(mTable);
	}

	/**
	 * Thêm những Suggest còn thiếu và SuggestCount
	 * 
	 * @throws Exception
	 */
	static void AddToSuggestCount() throws Exception
	{
		Vector<SuggestObject> mList_SuggestObj = Get_Current_SuggestObj();

		if (mList_SuggestObj.size() == Current_SuggestCountObj.size())
			return;
		else
		{
			// Thêm các SuggestCount chưa có ai mua trước đây (còn thiếu)
			for (SuggestObject mSuggestObj : mList_SuggestObj)
			{

				boolean Exist = false;
				for (SuggestCountObject mSuggestCountObj : Current_SuggestCountObj)
				{
					if (mSuggestObj.SuggestID == mSuggestCountObj.SuggestID)
					{
						Exist = true;
						break;
					}
				}
				if (Exist)
					continue;
				
				SuggestCountObject mObject = new SuggestCountObject();
				mObject.SuggestID = mSuggestObj.SuggestID;
				mObject.QuestionID = mSuggestObj.QuestionID;
				mObject.OrderNumber = mSuggestObj.OrderNumber;
				mObject.PlayDate = Get_Current_QuestionObj().PlayDate;
				mObject.LastUpdate = Calendar.getInstance().getTime();
				Current_SuggestCountObj.add(mObject);
			}
		}
	}

	private static Vector<SuggestCountObject> Current_SuggestCountObj = new Vector<SuggestCountObject>();

	/**
	 * lấy danh sách SuggestCount cho phiên hiện tại
	 * 
	 * @return
	 * @throws Exception
	 */
	public static synchronized Vector<SuggestCountObject> Get_Current_SuggestCountObj() throws Exception
	{
		if (Current_SuggestCountObj != null && Current_SuggestCountObj.size() > 0
				&& Current_SuggestCountObj.get(0).IsToday())
		{
			AddToSuggestCount();
			return Current_SuggestCountObj;
		}

		if (Current_SuggestCountObj != null)
		{
			Current_SuggestCountObj.clear();
		}

		SuggestCount mSuggestCount = new SuggestCount(LocalConfig.mDBConfig_MSSQL);
		MyTableModel mTable = mSuggestCount.Select(2, Integer.toString(Get_Current_QuestionObj().QuestionID));
		if (mTable != null && mTable.GetRowCount() > 1)
		{
			Current_SuggestCountObj = SuggestCountObject.ConvertToList(mTable);
		}

		AddToSuggestCount();
		return Current_SuggestCountObj;
	}

	/**
	 * Lấy 1 suggestCount theo SuggestID
	 * 
	 * @param SuggestID
	 * @return
	 * @throws Exception
	 */
	public static synchronized SuggestCountObject Get_SuggestCountObj(SuggestObject mSuggestObj) throws Exception
	{
		if (Get_Current_SuggestCountObj().size() < 1)
			return new SuggestCountObject();

		for (SuggestCountObject mObject : Get_Current_SuggestCountObj())
		{
			if (mObject.SuggestID == mSuggestObj.SuggestID)
				return mObject;
		}

		return new SuggestCountObject();
	}

	private static Vector<SuggestObject> Current_SuggestObj = new Vector<SuggestObject>();

	/**
	 * Lấy danh sách các dữ kiện cho câu hỏi hiện tại
	 * 
	 * @return
	 * @throws Exception
	 */
	public static synchronized Vector<SuggestObject> Get_Current_SuggestObj() throws Exception
	{

		if (Current_SuggestObj != null && Current_SuggestObj.size() == 20
				&& Current_SuggestObj.get(0).QuestionID == Get_Current_QuestionObj().QuestionID)
			return Current_SuggestObj;

		if (Current_SuggestObj != null)
			Current_SuggestObj.clear();

		Suggest mSuggest = new Suggest(LocalConfig.mDBConfig_MSSQL);

		MyTableModel mTable = mSuggest.Select(2, Integer.toString(Get_Current_QuestionObj().QuestionID));

		Current_SuggestObj = SuggestObject.ConvertToList(mTable);
		return Current_SuggestObj;
	}

	public static synchronized SuggestObject Get_SuggestObj(int OrderNuber) throws Exception
	{
		if (Get_Current_SuggestObj().size() < 1)
			return new SuggestObject();

		for (SuggestObject mObject : Get_Current_SuggestObj())
		{
			if (mObject.OrderNumber == OrderNuber)
				return mObject;
		}

		return new SuggestObject();
	}

	public static synchronized SuggestObject Get_SuggestObj_BuyID(int SuggestID) throws Exception
	{
		SuggestObject mSuggestObj = new SuggestObject();

		for (SuggestObject mObject : Get_Current_SuggestObj())
		{
			if (mObject.SuggestID == SuggestID)
				mSuggestObj = mObject;
		}

		if (mSuggestObj.IsNull())
		{
			Suggest mSuggest = new Suggest(LocalConfig.mDBConfig_MSSQL);

			MyTableModel mTable = mSuggest.Select(1, Integer.toString(SuggestID));
			if (mTable != null && mTable.GetRowCount() > 0)
			{
				mSuggestObj = SuggestObject.Convert(mTable);
				Get_Current_SuggestObj().add(mSuggestObj);
			}
		}
		return mSuggestObj;
	}

}

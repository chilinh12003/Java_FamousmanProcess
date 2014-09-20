package pro.check;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Vector;

import pro.define.PushMTObject;
import pro.server.Common;
import pro.server.LocalConfig;
import pro.server.Program;
import uti.utility.MyConfig.ChannelType;
import uti.utility.MyLogger;
import dat.content.DefineMT.MTType;
import dat.content.News;
import dat.content.News.NewsType;
import dat.content.NewsObject;
import dat.history.MOLog;
import dat.history.MOObject;
import dat.sub.Subscriber;
import dat.sub.SubscriberObject;
import db.define.MyDataRow;
import db.define.MyTableModel;

public class PushMT extends Thread
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());

	public PushMTObject mPushMTObj = new PushMTObject();

	Subscriber mSub = null;
	MOLog mMOLog = null;
	MyTableModel mTable_MOLog = null;

	public SimpleDateFormat DateFormat_InsertDB = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public PushMT()
	{

	}

	public PushMT(PushMTObject mPushMTObj)
	{
		this.mPushMTObj = mPushMTObj;

	}

	public void run()
	{
		if (Program.processData)
		{
			try
			{
				mSub = new Subscriber(LocalConfig.mDBConfig_MSSQL);
				mMOLog = new MOLog(LocalConfig.mDBConfig_MSSQL);
				mTable_MOLog = mMOLog.Select(0);

				PushForEach();

				UpdateNewsStatus(mPushMTObj.mNewsObj);
			}
			catch (Exception ex)
			{
				mLog.log.error("Loi xay ra trong qua trinh PUSH MT, Thead Index:" + mPushMTObj.ProcessIndex, ex);
			}
		}
	}

	private void UpdateNewsStatus(NewsObject mNewsObj)
	{
		try
		{
			News mNews = new News(LocalConfig.mDBConfig_MSSQL);
			MyTableModel mTable = mNews.Select(0);
			MyDataRow mRow = mTable.CreateNewRow();
			mRow.SetValueCell("NewsID", mNewsObj.NewsID);
			mRow.SetValueCell("StatusID", News.Status.Complete.GetValue());
			mRow.SetValueCell("StatusName", News.Status.Complete.toString());

			mTable.AddNewRow(mRow);
			mNews.Update(1, mTable.GetXML());

		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	private boolean PushForEach() throws Exception
	{
		MyTableModel mTable = new MyTableModel(null, null);
		Vector<SubscriberObject> mList = new Vector<SubscriberObject>();
		try
		{
			Integer MinPID = 0;

			for (Integer PID = MinPID; PID <= LocalConfig.MAX_PID; PID++)
			{
				mPushMTObj.CurrentPID = PID;
				mPushMTObj.MaxOrderID = 0;

				mTable = GetSubscriber(PID);

				while (!mTable.IsEmpty())
				{
					mList = SubscriberObject.ConvertToList(mTable, false);

					for (SubscriberObject mSubObj : mList)
					{
						// Nếu bị dừng đột ngột
						if (!Program.processData)
						{
							Insert_MOLog();
							mLog.log.debug("Bi dung Charge: Charge Info:" + mPushMTObj.GetLogString(""));
							return false;
						}

						mPushMTObj.MaxOrderID = mSubObj.OrderID;
					
						if (!SendMT(mSubObj))
						{
							mLog.log.info("Pust MT Fail -->MSISDN:" + mSubObj.MSISDN + "|NewsID:"
									+ mPushMTObj.mNewsObj.NewsID);
							MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_PushMT_NotSend",
									"PUSH MT FAIL --> MSISDN:" + mSubObj.MSISDN + "|NewsID:"
											+ mPushMTObj.mNewsObj.NewsID);
						}
						else
						{
							mLog.log.info("Pust MT OK -->MSISDN:" + mSubObj.MSISDN + "|NewsID:"
									+ mPushMTObj.mNewsObj.NewsID);
						}
						
						if(mPushMTObj.DelaySendMT > 0)
						{
							mLog.log.info("PushMT Delay: " + Integer.toString(mPushMTObj.DelaySendMT));
							Thread.sleep(mPushMTObj.DelaySendMT);
						}
					}
					Insert_MOLog();
					mTable.Clear();
					mTable = GetSubscriber(PID);
				}
			}
			return true;
		}
		catch (Exception ex)
		{
			mLog.log.debug("Loi trong PUSH MT cho dich vu");
			throw ex;
		}
		finally
		{
			Insert_MOLog();

			// Cập nhật thời gian kết thúc bắn tin
			mPushMTObj.FinishDate = Calendar.getInstance().getTime();
			mLog.log.debug("KET THUC PUSH MT");
		}
	}

	private boolean SendMT(SubscriberObject mSubObj)
	{
		try
		{
			String REQUEST_ID = Long.toString(System.currentTimeMillis());
			if (Common.SendMT(mSubObj.MSISDN, "", mPushMTObj.mNewsObj.MT, REQUEST_ID))
			{
				if(mPushMTObj.mNewsObj.mNewsType == NewsType.Push)
				AddToMOLog(mSubObj, MTType.PushMT,mPushMTObj.mNewsObj.MT, REQUEST_ID);
				else
					AddToMOLog(mSubObj, MTType.Reminder,mPushMTObj.mNewsObj.MT, REQUEST_ID);
				return true;
			}
			return false;
		}
		catch (Exception ex)
		{
			mLog.log.error("Gui MT khong thanh cong: MSISDN:" + mSubObj.MSISDN + "||NewsID:"
					+ mPushMTObj.mNewsObj.NewsID, ex);
		}
		return false;
	}
	
	private void AddToMOLog(SubscriberObject mSubObj,MTType mMTType_Current, String MTContent_Current, String REQUEST_ID) throws Exception
	{
		try
		{
			MOObject mMOObj = new MOObject(mSubObj.MSISDN,ChannelType.SYSTEM,
					mMTType_Current, "PUSH MT", MTContent_Current, REQUEST_ID,
					mSubObj.PID, Calendar.getInstance().getTime(),
					Calendar.getInstance().getTime(), mSubObj.mVNPApp, mSubObj.UserName, mSubObj.IP, mSubObj.PartnerID);

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
			if (mTable_MOLog.IsEmpty()) return;
			mMOLog.Insert(0, mTable_MOLog.GetXML());
			mTable_MOLog.Clear();
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	/**
	 * Lấy dữ liệu từ database
	 * 
	 * @return
	 * @throws Exception
	 */
	public MyTableModel GetSubscriber(Integer PID) throws Exception
	{
		try
		{
			return mSub.Select(5, mPushMTObj.RowCount.toString(), PID.toString(), mPushMTObj.MaxOrderID.toString(),
					mPushMTObj.ProcessNumber.toString(), mPushMTObj.ProcessIndex.toString());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

}

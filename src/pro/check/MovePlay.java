package pro.check;

import java.util.Calendar;
import java.util.Date;
import java.util.Vector;

import pro.server.LocalConfig;
import pro.server.Program;
import uti.utility.MyLogger;
import dat.history.Play;
import dat.history.PlayLog;
import dat.history.PlayObject;
import dat.history.SuggestCount;
import dat.history.SuggestCountLog;
import db.define.MyTableModel;

public class MovePlay extends Thread
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());

	public int CurrentPID = 1;

	public String MSISDN = "";

	/**
	 * Số lượng process Push MT được tạo ra
	 */
	public int ProcessNumber = 1;

	/**
	 * Thứ tự của 1 process
	 */
	public int ProcessIndex = 0;

	/**
	 * Số thứ tự (OrderID) trong table Subscriber, process sẽ lấy những record
	 * có OrderID >= MaxOrderID
	 */
	public long MaxOrderID = 0;

	/**
	 * Tổng số record mỗi lần lấy lên để xử lý
	 */
	public int RowCount = 10;

	/**
	 * Thời gian bắt đầu chạy thead
	 */
	public Date StartDate = null;

	/**
	 * Thời gian kết thúc chạy thead
	 */
	public Date FinishDate = null;

	public boolean IsNull()
	{
		if (MSISDN == "") return true;
		else return false;
	}

	public String GetLogString(String Suffix) throws Exception
	{
		try
		{
			if (IsNull()) return "";
			String Fomart = "MSISDN:%s || ProcessIndex:%s || CurrentPID:%s || MaxOrderID:%s || Suffix:%s";
			return String.format(Fomart, new Object[]{MSISDN, ProcessIndex, CurrentPID, MaxOrderID, Suffix});
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

	public MovePlay()
	{

	}

	Integer TotalCount = 0;

	Play mPlay = null;
	PlayLog mPlayLog = null;
	SuggestCount mSuggestCount = null;
	SuggestCountLog mSuggestCountLog = null;

	MyTableModel mTable_Play = null;
	MyTableModel mTable_SuggestCount = null;
	public void run()
	{
		if (Program.processData)
		{
			try
			{
				mPlay = new Play(LocalConfig.mDBConfig_MSSQL);
				mPlayLog = new PlayLog(LocalConfig.mDBConfig_MSSQL);
				mSuggestCount = new SuggestCount(LocalConfig.mDBConfig_MSSQL);

				mSuggestCount = new SuggestCount(LocalConfig.mDBConfig_MSSQL);
				mSuggestCountLog = new SuggestCountLog(LocalConfig.mDBConfig_MSSQL);

				mTable_Play = mPlay.Select(0);
				mTable_SuggestCount = mSuggestCount.Select(0);

				ForEach_Play();
			}
			catch (Exception ex)
			{
				mLog.log.error("Loi xay ra trong qua trinh Move to PlayLog,SuggestCountLog, Thead Index:"
						+ this.ProcessIndex, ex);
			}

		}
	}

	/**
	 * Lấy dữ liệu Play
	 * 
	 * @return
	 * @throws Exception
	 */
	public MyTableModel GetPlay() throws Exception
	{
		try
		{
			// Lấy danh sách(Para_1 = RowCount, Para_2 = LogID,
			// Para_3 = ProcessNumber, Para_4 = ProcessIndex )
			return mPlay.Select(3,Integer.toString(RowCount), Long.toString(MaxOrderID), Integer.toString(ProcessNumber),
					Integer.toString(ProcessIndex));
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}	

	private void ForEach_Play() throws Exception
	{
		MyTableModel mTable = new MyTableModel(null, null);
		Vector<PlayObject> mList = new Vector<PlayObject>();
		try
		{

			MaxOrderID = 0;
			Integer TotalCount = 0;
			mTable = GetPlay();

			while (!mTable.IsEmpty())
			{
				MaxOrderID = Long.parseLong(mTable.GetValueAt(mTable.GetRowCount() - 1, "LogID").toString());

				if (mPlayLog.Insert(0, mTable.GetXML()))
				{
					mPlay.Delete(0, mTable.GetXML());
				}
				TotalCount += mTable.GetRowCount();

				mLog.log.debug("Xu ly xong tong so row:" + TotalCount.toString());
				mTable = GetPlay();
			}
			return;
		}
		catch (Exception ex)
		{
			mLog.log.debug("Loi trong di chuyen du lieu Play --> PlayLog", ex);
			throw ex;
		}
		finally
		{
			mList.clear();

			FinishDate = Calendar.getInstance().getTime();
			mLog.log.debug("Ket thuc di chuyen Play --> PlayLog");
		}
	}

}
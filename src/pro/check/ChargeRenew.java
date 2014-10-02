package pro.check;

import java.util.Calendar;
import java.util.Vector;

import pro.charge.Charge;
import pro.charge.Charge.ErrorCode;
import pro.charge.Charge.Reason;
import pro.define.ChargeThreadObject;
import pro.define.ChargeThreadObject.ThreadStatus;
import pro.server.Common;
import pro.server.CurrentData;
import pro.server.LocalConfig;
import pro.server.Program;
import uti.utility.MyConfig;
import uti.utility.MyConfig.ChannelType;
import uti.utility.VNPApplication;
import uti.utility.MyDate;
import uti.utility.MyLogger;
import dat.content.DefineMT.MTType;
import dat.history.ChargeLog;
import dat.history.MOLog;
import dat.history.MOObject;
import dat.sub.Subscriber;
import dat.sub.Subscriber.Status;
import dat.sub.SubscriberObject;
import dat.sub.UnSubscriber;
import db.define.MyDataRow;
import db.define.MyTableModel;

/**
 * Thread sẽ bắn tin cho từng dịch vụ
 * 
 * @author Administrator
 * 
 */
public class ChargeRenew extends Thread
{
	MyLogger mLog = new MyLogger(LocalConfig.LogConfigPath, this.getClass().toString());

	public ChargeThreadObject mCTObject = new ChargeThreadObject();

	public ChargeRenew()
	{

	}

	public ChargeRenew(ChargeThreadObject mCTObject)
	{
		this.mCTObject = mCTObject;
	}

	Subscriber mSub = null;
	UnSubscriber mUnSub = null;
	MOLog mMOLog = null;
	ChargeLog mChargeLog = null;

	MyTableModel mTable_SubUpdate = null;
	MyTableModel mTable_ChargeLog = null;
	MyTableModel mTable_MOLog = null;
	pro.charge.Charge mCharge = new Charge();

	int TotalCount = 0;

	// public SimpleDateFormat MyConfig.Get_DateFormat_InsertDB() = new
	// SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	public void run()
	{
		if (Program.processData)
		{
			try
			{
				mCharge.AllowInsertChargeLog = false;

				mSub = new Subscriber(LocalConfig.mDBConfig_MSSQL);
				mUnSub = new UnSubscriber(LocalConfig.mDBConfig_MSSQL);
				mTable_SubUpdate = mSub.Select(0);
				mTable_SubUpdate.Clear();

				mChargeLog = new ChargeLog(LocalConfig.mDBConfig_MSSQL);
				mTable_ChargeLog = mChargeLog.Select(0);

				mMOLog = new MOLog(LocalConfig.mDBConfig_MSSQL);
				mTable_MOLog = mMOLog.Select(0);

				PushForEach();
			}
			catch (Exception ex)
			{
				mCTObject.mThreadStatus = ThreadStatus.Error;

				mLog.log.error("Loi xay ra trong qua trinh Charging, Thead Index:" + mCTObject.ProcessIndex, ex);
			}
		}
	}

	private boolean PushForEach() throws Exception
	{
		MyTableModel mTable = new MyTableModel(null, null);
		try
		{
			Integer MinPID = 0;

			if (mCTObject.CurrentPID > 0) MinPID = mCTObject.CurrentPID;

			for (Integer PID = MinPID; PID <= LocalConfig.MAX_PID; PID++)
			{
				mCTObject.CurrentPID = PID;
				mCTObject.MaxOrderID = 0;

				mTable = GetSubscriber(PID);

				while (!mTable.IsEmpty())
				{
					Vector<SubscriberObject> mListSubObj = SubscriberObject.ConvertToList(mTable, false);

					for (SubscriberObject mSubObj : mListSubObj)
					{
						// nếu bị dừng đột ngột
						if (!Program.processData)
						{
							mLog.log.debug("Bi dung Charge: Charge Info:" + mCTObject.GetLogString(""));

							mCTObject.mThreadStatus = ThreadStatus.Stop;
							mCTObject.QueueDate = Calendar.getInstance().getTime();

							Insert_ChargeLog();
							UpdateCharge();
							return false;
						}

						TotalCount++;

						mCTObject.MaxOrderID = mSubObj.OrderID;
						mCTObject.MSISDN = mSubObj.MSISDN;

						Calendar mCal_Current = Calendar.getInstance();
						Calendar mCal_ExpireDate = Calendar.getInstance();

						mCal_ExpireDate.setTime(mSubObj.ExpiryDate);

						Long CountDay = MyDate.diffDays(mCal_ExpireDate, mCal_Current);

						if (CountDay < 1)
						{
							// nếu chưa hết hạn thì không tiến hành xử
							// lý charge
							continue;
						}

						ErrorCode mResultCharge = mCharge.ChargeRenew(mSubObj, ChannelType.SYSTEM, "RENEW");

						AddToChargeLog(mSubObj, 2000, pro.charge.Charge.Reason.RENEW, ChannelType.SYSTEM,
								mResultCharge);

						// Theo yêu cầu của VNP thì các trường hợp này phải hủy,
						// chi tiết hãy đọc tài liêu
						if (mResultCharge == ErrorCode.SubDoesNotExist)
						{
							mSubObj.RetryChargeDate = mCal_Current.getTime();

							// Hủy nhưng ko gửi MT
							DeregSub(mSubObj,ChannelType.SUBNOTEXIST, false);
							continue;
						}

						if (mResultCharge != ErrorCode.ChargeSuccess && mSubObj.mStatus == Status.ChargeFail
								&& CountDay >= LocalConfig.CHARGE_MAX_DAY_RETRY && mCTObject.AllowDereg)
						{
							mSubObj.RetryChargeDate = mCal_Current.getTime();

							DeregSub(mSubObj,ChannelType.MAXRETRY, true);
							continue;
						}

						if (mResultCharge == ErrorCode.ChargeSuccess)
						{
							//Nếu lần charge trước không thành công, và cho phép push MT vào khung giờ charge
							//Thì tiến hành gửi MT thông báo cho KH, để KH biết và tiếp tục chơi
							if(mSubObj.mStatus == Status.ChargeFail && mCTObject.AllowDereg)
							{
								SendMT_NotifySuccess(mSubObj);
							}
							
							mCal_ExpireDate.set(Calendar.MILLISECOND, 0);
							mCal_ExpireDate.set(mCal_Current.get(Calendar.YEAR), mCal_Current.get(Calendar.MONTH),
									mCal_Current.get(Calendar.DATE), 23, 59, 59);

							mSubObj.ExpiryDate = mCal_ExpireDate.getTime();
							mSubObj.RenewChargeDate = mCal_Current.getTime();
							mSubObj.RetryChargeCount = 0;
							mSubObj.mStatus = Status.Active;
							// Tăng số MT bắn thành công
							mCTObject.SuccessNumber++;
						}
						else
						{
							mSubObj.RetryChargeDate = mCal_Current.getTime();

							mSubObj.RetryChargeCount++;
							mSubObj.mStatus = Status.ChargeFail;

							// Tăng số MT bắn không thành công
							mCTObject.FailNumber++;

							// Ghi lại các trường hợp chưa bắn được MT
							// để sau này push lại
							mCTObject.QueueDate = Calendar.getInstance().getTime();
						}

						mTable_SubUpdate = mSubObj.AddNewRow(mTable_SubUpdate);
					}
					mLog.log.debug("Tien Hanh charge cho:" + TotalCount + " thue bao ProcessIndex:"
							+ mCTObject.ProcessIndex);
					Insert_ChargeLog();
					UpdateCharge();

					mTable.Clear();
					mTable = GetSubscriber(PID);
				}
			}
			mCTObject.mThreadStatus = ThreadStatus.Complete;
			return true;
		}
		catch (Exception ex)
		{
			mLog.log.error("Loi trong charge renew cho dich vu", ex);
			throw ex;
		}
		finally
		{
			Insert_ChargeLog();
			UpdateCharge();
			// Cập nhật thời gian kết thúc bắn tin
			mCTObject.FinishDate = Calendar.getInstance().getTime();

			mLog.log.debug("KET THUC CHARGING ProcessIndex:" + mCTObject.ProcessIndex + "|PID:" + mCTObject.CurrentPID
					+ "|OrderID:" + mCTObject.MaxOrderID + "|TotalCount:" + TotalCount);
		}
	}
	
	/**
	 * Gửi MT cho khách hàng khi charge thành công mà lần charge trước bị FAIL
	 * @param mSubObj
	 */
	private void SendMT_NotifySuccess(SubscriberObject mSubObj)
	{
		try
		{
			MTType mMTType = MTType.NotifyRenewSuccessBeforeFail;
			String COMMAND_CODE = "RenewSuccess";
			String REQUEST_ID = Long.toString(System.currentTimeMillis());
			String MTContent = Common.GetDefineMT_Message(mMTType);
			if (!MTContent.equalsIgnoreCase(""))
			{
				Common.SendMT(mSubObj.MSISDN, COMMAND_CODE, MTContent, REQUEST_ID);
			}
			Insert_MOLog(mSubObj,mMTType,ChannelType.SYSTEM,MTContent);
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}
	private void AddToChargeLog(SubscriberObject mSubObj, Integer Price, Reason mReason,
			MyConfig.ChannelType mChannelType, ErrorCode mResult)
	{
		try
		{
			Calendar CurrentDate = Calendar.getInstance();

			MyDataRow mRow_Log = mTable_ChargeLog.CreateNewRow();

			mRow_Log.SetValueCell("MSISDN", mSubObj.MSISDN);
			mRow_Log.SetValueCell("ChargeDate", MyConfig.Get_DateFormat_InsertDB().format(CurrentDate.getTime()));
			mRow_Log.SetValueCell("ChargeTypeID", mReason.GetValue());
			mRow_Log.SetValueCell("ChargeTypeName", mReason.toString());
			mRow_Log.SetValueCell("ChargeStatusID", mResult.GetValue());
			mRow_Log.SetValueCell("ChargeStatusName", mResult.toString());
			mRow_Log.SetValueCell("ChannelTypeID", mChannelType.GetValue());
			mRow_Log.SetValueCell("ChannelTypeName", mChannelType.toString());
			mRow_Log.SetValueCell("Price", Price);
			mRow_Log.SetValueCell("PID", mSubObj.PID);
			mRow_Log.SetValueCell("PartnerID", mSubObj.PartnerID);
			mTable_ChargeLog.AddNewRow(mRow_Log);

		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	private void Insert_ChargeLog() throws Exception
	{
		try
		{
			if (mTable_ChargeLog.GetRowCount() < 1)
				return;

			mChargeLog.Insert(0, mTable_ChargeLog.GetXML());
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
		finally
		{
			mTable_ChargeLog.Clear();
		}

	}

	private void Insert_MOLog(SubscriberObject mSubObj, MTType mMTType, ChannelType mChannelType, String MTContent)
			throws Exception
	{
		try
		{
			mTable_MOLog.Clear();

			MOObject mMOObj = new MOObject(mSubObj.MSISDN, mChannelType, mMTType, "", MTContent, "0",
					mSubObj.PID, Calendar.getInstance().getTime(), Calendar.getInstance().getTime(),
					new VNPApplication(), "", "", mSubObj.PartnerID);
			mTable_MOLog = mMOObj.AddNewRow(mTable_MOLog);
			mMOLog.Insert(0, mTable_MOLog.GetXML());
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
		finally
		{
			mTable_MOLog.Clear();
		}
	}
	/**
	 * Hủy dịch vụ một số thuê bao khi charge không thành công
	 */
	private void DeregSub(SubscriberObject mSubObj,ChannelType mChannelType, boolean AllowSendMT)
	{
		try
		{
			MyTableModel mTable_UnSub = CurrentData.GetTable_UnSub();

			mSubObj.DeregDate = Calendar.getInstance().getTime();
			
			mTable_UnSub = mSubObj.AddNewRow(mTable_UnSub);
			String XML = mTable_UnSub.GetXML();
			// Tiến hành hủy đăng ký khi mà retry không
			// thành công
			if (ErrorCode.ChargeSuccess != mCharge.ChargeDereg(mSubObj, mChannelType, "UNREG"))
			{
				MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_Sync_Dereg_VNP_FAIL",
						"DEREG RECORD FAIL --> " + XML);
			}

			if (mUnSub.Move(0, XML))
			{
				// Có những trường hợp Hủy nhưng ko cần gửi MT
				if (AllowSendMT)
				{
					String MTContent = Common.GetDefineMT_Message(MTType.DeregExtendFail);

					if (Common.SendMT(mCTObject, MTContent))
					{
						Insert_MOLog(mSubObj, MTType.DeregExtendFail, MyConfig.ChannelType.MAXRETRY,
								MTContent);
					}
				}
				else
				{
					MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_Sync_Dereg_NOT_SEND_MT", "INFO --> "
							+ XML);
				}
			}
			else
			{
				MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_NotMoveToUnSub", "DEREG RECORD FAIL --> "
						+ XML);
			}
		}
		catch (Exception ex)
		{
			mLog.log.error(ex);
		}
	}

	private void UpdateCharge() throws Exception
	{
		String XML = "";
		try
		{
			if (mTable_SubUpdate.IsEmpty())
				return;

			XML = mTable_SubUpdate.GetXML();

			if (!mSub.UpdateCharge(0, XML))
			{
				MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_NotUpdateDB", "LIST RECORD --> " + XML);
			}

		}
		catch (Exception ex)
		{
			MyLogger.WriteDataLog(LocalConfig.LogDataFolder, "_Charge_NotUpdateDB", "LIST RECORD --> " + XML);
			mLog.log.error(ex);
		}
		finally
		{
			mTable_SubUpdate.Clear();
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
			// Lấy danh sách(Para_1 = RowCount, Para_2 = PID, Para_3 = OrderID,
			// Para_4 = ProcessNumber, Para_5 = ProcessIndex )
			return mSub.Select(5, mCTObject.RowCount.toString(), PID.toString(), mCTObject.MaxOrderID.toString(),
					mCTObject.ProcessNumber.toString(), mCTObject.ProcessIndex.toString());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}

}

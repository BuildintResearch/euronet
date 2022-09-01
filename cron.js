const db = require("../config/connection");
const {
  expandeddata, systemcontrol, rawdata,
  hourlydata, auditDevStatus, transAlertStatus,
  expanded_data_latest, transCommand, device, upsLogs, location, dailyreport, upsChart, siteAlert
} = db;
const fs = require('fs');
const cronmaintenence = require("../app/cronmaintenence/models/cronmaintenence");
const NotificationsService = require("../app/notifications/models/notifications");
let cronmaintenenceObj = new cronmaintenence();
const winston = require(ROOT_DIR + "/winstonlog.js");
const dataLog = require(ROOT_DIR + "/dataLog.js");
const currency = require('currency.js');
const moment = require('moment');
const cron = require("cron").CronJob;
const Constants = require("./../Constants");
const request = require("./../services/request");
const { dbConfig } = require("../config")
// const Queue = require("./../lib/queue");
const { Op } = require("sequelize");
// const deviceData = []
let { storeData } = require('../util')
let expandedDataObj = [];
let processedData = [];
let tempDevID = [];
const fetch = require('node-fetch')

// const emialQueue = new Queue();

const allcron = {};
const DeviceSignageStatus = {};



// setInterval(() => {
//   const noti = emialQueue.dequeue();
//   if (noti) NotificationsService.create(noti);
// }, 10000);

const check_KWH_ValueIs0 = (rawDatas) => {
  if (rawDatas[2].split(":")[1] == 0 && rawDatas[3].split(":")[1] == 0 && rawDatas[4].split(":")[1] == 0 && rawDatas[5].split(":")[1] == 0) return 1
  return 0
}

const checkDeviceSignageStatus = async ({ evt_dt, DID, SIGN_status }) => {
  try {
    const updateKey = SIGN_status == 3 ? 'signage_end_time' : 'signage_st_time'
    const dev_id = DID.split('').splice(0, DID.length - 1).join('')
    if (!DeviceSignageStatus[dev_id]) {
      const locDetails = await db.sequelize.query(
        `SELECT * FROM mst_location where dev_id=:dev_id `,
        { type: db.sequelize.QueryTypes.SELECT, replacements: { dev_id } }
      );

      if (Array.isArray(locDetails) && locDetails.length) {
        const {
          signage_st_time = new Date(),
          signage_end_time = new Date(),
          signage_status = null
        } = locDetails[0];

        const signageEvtDt = (new Date(signage_st_time) > new Date(signage_end_time) ?
          new Date(signage_end_time) : new Date(signage_st_time))

        DeviceSignageStatus[dev_id] = {
          evt_dt: evt_dt,
          // evt_dt: signageEvtDt == 'Invalid Date' ? evt_dt : signageEvtDt,
          dev_id, DID, SIGN_status: signage_status
        }
        await db.sequelize.query(`
        UPDATE  mst_location 
        SET ${updateKey}=:evt_dt ,signage_status=:SIGN_status
        WHERE dev_id=:dev_id`,
          { type: db.sequelize.QueryTypes.UPDATE, replacements: { evt_dt, dev_id, SIGN_status } })
      }
    } else if (DeviceSignageStatus[dev_id] && DeviceSignageStatus[dev_id].SIGN_status !== SIGN_status) {
      await db.sequelize.query(`
                UPDATE  mst_location 
                SET ${updateKey}=:evt_dt, signage_status=:SIGN_status
                WHERE dev_id=:dev_id`,
        {
          type: db.sequelize.QueryTypes.UPDATE,
          replacements: { evt_dt, dev_id, SIGN_status }
        })
      DeviceSignageStatus[dev_id] = {
        evt_dt, dev_id, DID, SIGN_status
      }
    }
  } catch (error) {
    console.log(error);
    winston.error(`=================== checkDeviceSignageStatus Error ${error.stack}`)
  }
}


const creatAlertNotificationToOrg = async ({
  rxtype = [], message = '', DID = '', priority, severity, columnValue, loc_id, alertType, loc_name, rawDataID = '', istdate = '', alertTypeId = ''
}) => {
  try {
    let dev_id = DID
    let project = "Euronet"
    if (DID.charAt(DID.length - 1) == "A" || DID.charAt(DID.length - 1) == "S") {
      dev_id = DID.split('').splice(0, DID.length - 1).join('')
      project = "IDFC"
    }
    // if (dbConfig.database == "db_icontroll") {
    //   dev_id = DID.split('').splice(0, DID.length - 1).join('')
    // }
    // 'Critical, Major, Minor, Info
    // 'High, Medium,  Low
    //DID.split('').splice(0, DID.length - 1).join('')
    // const dev_id = DID
    console.log('========Inside createAlertNotification============================');
    console.log('====================================');
    if (!loc_id) {
      const locDetails = await db.sequelize.query(`SELECT * FROM mst_location where dev_id='${dev_id}' and is_active=1`, { type: db.sequelize.QueryTypes.SELECT });
      if (!locDetails || (Array.isArray(locDetails) && !locDetails.length)) return
      loc_id = locDetails[0].loc_id
      loc_name = locDetails[0].name
    }

    // TODO insert in to site alert
    var minutesToAdd = 330;
    var currentDate = new Date();
    var futureDate = new Date(currentDate.getTime() + minutesToAdd * 60000).toJSON().slice(0, 19).replace('T', ' ');
    if (istdate) currentDate = istdate
    // var raisedOnDate = new Date(currentDate.getTime() + minutesToAdd * 60000).toJSON().slice(0, 19).replace('T', ' ')
    var raisedOnDate = new Date(currentDate).toJSON().slice(0, 19).replace('T', ' ')
    //const nowDate = new Date().toJSON().slice(0, 19).replace('T', ' ');
    if (!istdate) raisedOnDate = futureDate
    const nowDate = futureDate;
    const siteAlrtNew = `INSERT INTO site_alerts (location,issue,alert_id,is_active,raised_on,createdAt,updatedAt,Loc_name,observed_val,severity,priority,rawData_id,alert_type) VALUES
                     (${loc_id},'${message}',${Constants.alertStatusId[alertType]},1,'${raisedOnDate}','${nowDate}','${nowDate}', '${loc_name}','${columnValue}','${severity}','${priority}','${rawDataID}','${alertTypeId}')`
    console.log('=================siteAlrtNew===================');
    console.log(siteAlrtNew);
    console.log('====================================');
    const alertDoc = await db.sequelize.query(siteAlrtNew, { type: db.sequelize.QueryTypes.INSERT });

    if (severity == 'High') {
      winston.info("High ");
      const orgList = await db.sequelize.query(`SELECT * FROM mst_org where is_active=1`, { type: db.sequelize.QueryTypes.SELECT });
      if (rxtype.length) {
        orgList.forEach(async ele => {
          let email = []
          let ccEmail = []
          if (rxtype.includes('internal')) {
            let int_mails = ele.int_team_emails ? JSON.parse(ele.int_team_emails) : null
            if (int_mails) {
              if (int_mails[alertType].email && int_mails[alertType].email.length)
                email.push(...int_mails[alertType].email);
              if (int_mails[alertType].cc && int_mails[alertType].cc.length)
                ccEmail.push(...int_mails[alertType].cc)
            }
          }

          if (rxtype.includes('busi')) {
            let bus_mails = ele.bus_team_emails ? JSON.parse(ele.bus_team_emails) : null
            if (bus_mails) {
              if (bus_mails[alertType].email && bus_mails[alertType].email.length)
                email.push(...bus_mails[alertType].email);
              if (bus_mails[alertType].cc && bus_mails[alertType].cc.length)
                ccEmail.push(...bus_mails[alertType].cc)
            }
          }

          if (email.length) {
            const temp = Constants.alertTemplate
              .replace('{TICKED_ID}', `${project}_${alertDoc[0]}`)
              .replace('{ALERT_MSG}', `${message} Issue in  ${loc_name}(severity: ${severity}).`)
              .replace('{SUPPORT_SRV}', 'NONE')
              .replace('{ALERT_LVL}', `You have an${severity == 'High' ? ' ' : ' NON '}Critical Alert`)

            NotificationsService.create({
              noti_type: Constants.notificationType.email,
              email: email.join(','),
              cc: ccEmail.length ? ccEmail.join(',') : "",
              msg: `[${severity}][${priority}][${DID}] ${message}`,
              data: temp,
              user_id: loc_id,
              msg_id: alertDoc[0].id,
              msg_type: alertType,
            });
          }

        })
      }
    }
    return alertDoc[0];
  } catch (error) {
    console.log(error);
    winston.error(`=================== creatAlertNotificationToOrg Error ${error.stack}`)
  }

}

allcron.creatAlertNotificationToOrg = creatAlertNotificationToOrg;
const tempCheckAndSendAlert = async ({ rawDataID, istdate, TMValue, DID }) => {
  try {
    const alertType = Constants.alertTypeId.Temp
    const created_by = `tempCheckAndSendAlert_`
    const TM = TMValue
    const maxThresholdValue = 30;
    const minThresholdValue = 24;
    try {
      const columns = [];
      let errorFlag = false;
      if (TM < minThresholdValue) {
        columns.push(`Less Then ${minThresholdValue}`)
      }
      if (TM > maxThresholdValue) {
        columns.push(`Greater Then ${maxThresholdValue}`)
      }
      const recordStatus = columns.length ? 1 : 0
      const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
        where: { DID: DID, alert_type: alertType },
        defaults: {
          DID: DID, alert_type: alertType,
          is_active: recordStatus, created_by
        }
      });
      // let issue_text = "Temperature is " + TM;
      // const nowDate = new Date().toJSON().slice(0, 19).replace('T', ' ');
      // const siteAlrtNew = `INSERT INTO site_alerts (location,issue,alert_id,is_active,raised_on,
      //   createdAt,updatedAt,Loc_name,observed_val,severity,priority) VALUES
      //      (${DID}, '${issue_text}',4,1,'${nowDate}','${nowDate}',
      //      '${nowDate}', '${DID}',${TM},'LOW', 'LOW')`
      // await db.sequelize.query(siteAlrtNew, { type: db.sequelize.QueryTypes.INSERT })

      if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
        transAlertStatusDoc.is_active = 0;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
      } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
        transAlertStatusDoc.is_active = 1;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
        errorFlag = true;
      }

      if (errorFlag || (docStatus && recordStatus)) {
        const rxTy = ['internal']
        if (TM > 18) { rxTy.push('busi') }
        await creatAlertNotificationToOrg({
          message: `Temprature is ${columns.join('')}`,
          alertType: Constants.alertStatus.Temp,
          DID,
          columnValue: TM,
          rxtype: rxTy,
          severity: 'Minor', priority: 'Low',
          istdate,
          rawDataID
        })
      }
    } catch (error) {
      winston.info(`Cron tempCheckAndSendAlert error----->", ${error.stack}`);
    }
  } catch (err) {
    winston.info(`Cron tempCheckAndSendAlert error----->", ${err.stack}`);
    console.error(err)
  }
};

const euronet_tempCheckAndSendAlert = async ({ rawDataID, istdate, TMValue, DID, loc_id = '' }) => {
  try {
    const alertType = Constants.alertTypeId.Temp
    const created_by = `tempCheckAndSendAlert_`
    const TM = TMValue
    try {
      let errorFlag = false;
      const recordStatus = TMValue > 30 ? 1 : 0
      const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
        where: { DID: DID, alert_type: alertType },
        defaults: {
          DID: DID, alert_type: alertType,
          is_active: recordStatus, created_by
        }
      });
      // let issue_text = "Temperature is " + TM;
      // const nowDate = new Date().toJSON().slice(0, 19).replace('T', ' ');
      // const siteAlrtNew = `INSERT INTO site_alerts (location,issue,alert_id,is_active,raised_on,
      //   createdAt,updatedAt,Loc_name,observed_val,severity,priority) VALUES
      //      (${DID}, '${issue_text}',4,1,'${nowDate}','${nowDate}',
      //      '${nowDate}', '${DID}',${TM},'LOW', 'LOW')`
      // await db.sequelize.query(siteAlrtNew, { type: db.sequelize.QueryTypes.INSERT })

      if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
        transAlertStatusDoc.is_active = 0;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
        if (loc_id) await siteAlert.update({ is_active: 3 }, { where: { alert_type: alertType, location: loc_id, severity: Constants.priorityStatus.Minor, priority: Constants.severityStatus.Low } })
      } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
        transAlertStatusDoc.is_active = 1;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
        errorFlag = true;
      }

      if (errorFlag || (docStatus && recordStatus)) {
        const rxTy = ['internal']
        if (TM > 18) { rxTy.push('busi') }
        await creatAlertNotificationToOrg({
          message: `Current Temperature:${TMValue} is greater than set Temperature 30`,
          alertType: Constants.alertStatus.Temp,
          cc: "tushar.k@cabbagesoft.com",
          DID,
          columnValue: TM,
          rxtype: rxTy,
          severity: 'Minor', priority: 'Low',
          istdate,
          rawDataID,
          alertTypeId: alertType
        })
      }
    } catch (error) {
      winston.info(`Cron tempCheckAndSendAlert error----->", ${error.stack}`);
    }
  } catch (err) {
    winston.info(`Cron tempCheckAndSendAlert error----->", ${err.stack}`);
    console.error(err)
  }
};

const acCheckAndSendAlert = async ({ HAone, HAtwo, DID, Oprator }) => {
  try {
    let alertType = Constants.alertTypeId.AC;
    const created_by = `acCheckAndSendAlert_`
    const thresholdValue = 0;
    try {
      const columns = [];
      let errorFlag = false;

      switch (Oprator) {
        case 'OR':
          if (HAone > 3) {
            columns.push('AC-1')
          }
          if (HAtwo > 3) {
            columns.push('AC-2')
          }
          alertType = Constants.alertTypeId.AC
          break;
        case 'AND':
          if (HAone <= thresholdValue && HAtwo <= thresholdValue) {
            columns.push('AC-1')
            columns.push('AC-2')
          }
          break;
        default:
          break;
      }
      const recordStatus = columns.length == 2 ? 1 : 0
      const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
        where: { DID: DID, alert_type: alertType },
        defaults: {
          DID: DID, alert_type: alertType,
          is_active: recordStatus, created_by
        }
      });

      if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
        transAlertStatusDoc.is_active = 0;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
      } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
        transAlertStatusDoc.is_active = 1;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
        errorFlag = true;
      }
      if (errorFlag || (docStatus && recordStatus)) {
        creatAlertNotificationToOrg({
          message: `${columns.join(',')} value gone less then ${thresholdValue}`,
          alertType: Constants.alertStatus.AC,
          columnValue: `HAone:${HAone},HAtwo:${HAtwo}`,
          DID, rxtype: ['internal', 'busi'],
          severity: 'Minor', priority: 'Low',
          istdate,
          rawDataID
        })
      }
    } catch (error) {
      winston.info(`Cron tempCheckAndSendAlert error----->", ${error.stack}`);
    }
  } catch (err) {
    winston.info(`Cron tempCheckAndSendAlert error----->", ${err.stack}`);
    console.error(err)
  }
};

const acLoadConsumption = async ({ rawDataID, istdate, Type, Status, caValue, DID, loc_id = '' }) => {
  try {
    if (Status) {
      const created_by = `acLoadConsumption`
      let alertType = Type == 1 ? Constants.alertTypeId.AC1_Load_consumption : Constants.alertTypeId.AC2_Load_consumption
      let acType = Type == 1 ? "AC1" : "AC2";
      const maxThresholdValue = 6;
      const minThresholdValue = 3.5;
      try {
        const columns = [];
        let errorFlag = false;
        if (caValue < minThresholdValue) {
          columns.push(`Less Then ${minThresholdValue}`)
        }
        if (caValue > maxThresholdValue) {
          columns.push(`Greater Then ${maxThresholdValue}`)
        }
        const recordStatus = columns.length ? 1 : 0
        const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
          where: { DID: DID, alert_type: alertType },
          defaults: {
            DID: DID, alert_type: alertType,
            is_active: recordStatus, created_by
          }
        });

        if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
          transAlertStatusDoc.is_active = 0;
          transAlertStatusDoc.updated_by = created_by;
          await transAlertStatusDoc.save();
          if (loc_id) await siteAlert.update({ is_active: 3 }, { where: { alert_type: alertType, location: loc_id, severity: Constants.priorityStatus.Minor, priority: Constants.severityStatus.Low } })

        } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
          transAlertStatusDoc.is_active = 1;
          transAlertStatusDoc.updated_by = created_by;
          await transAlertStatusDoc.save();
          errorFlag = true;
        }

        if (errorFlag || (docStatus && recordStatus)) {
          const rxTy = ['internal']
          await creatAlertNotificationToOrg({
            message: `${acType} Load Consumption is ${columns.join('')} Amp,It should be between 3.5 to 6 Amp`,
            alertType: Constants.alertStatus.AC,
            cc: "tushar.k@cabbagesoft.com",
            DID,
            columnValue: caValue,
            rxtype: rxTy,
            severity: 'Minor', priority: 'Low',
            istdate,
            rawDataID,
            alertTypeId: alertType
          })
        }
      } catch (error) {
        winston.info(`Cron tempCheckAndSendAlert error----->", ${error.stack}`);
      }
    }
  } catch (err) {
    winston.info(`Cron tempCheckAndSendAlert error----->", ${err.stack}`);
    console.error(err)
  }
};

const upsPower_off = async ({ rawDataID, istdate, VU, DID }) => {
  //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
  const created_by = `upsPower_off`
  let errorFlag = false
  let recordStatus = VU == 0 ? 1 : 0
  const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
    where: { DID: DID, alert_type: Constants.alertTypeId.UPS_Power_Off },
    defaults: {
      DID: DID, alert_type: Constants.alertTypeId.UPS_Power_Off,
      is_active: recordStatus, created_by
    }
  });

  if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
    transAlertStatusDoc.is_active = 0;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
  } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
    transAlertStatusDoc.is_active = 1;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
    errorFlag = true;
  }

  if (errorFlag || (docStatus && recordStatus)) {
    await creatAlertNotificationToOrg({
      message: `UPS is Not Working`,
      alertType: Constants.alertStatus.UPS,
      cc: "tushar.k@cabbagesoft.com",
      DID,
      columnValue: VU,
      rxtype: ['internal'],
      severity: 'High',
      priority: 'High',
      istdate,
      rawDataID
    })
  }
}

const rawPower_off = async ({ rawDataID, istdate, VN, VE, DID }) => {
  //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
  const created_by = `rawPower_off`
  let errorFlag = false
  let recordStatus = VE && VN == 0 ? 1 : 0
  const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
    where: { DID: DID, alert_type: Constants.alertTypeId.RAW_Power_Off },
    defaults: {
      DID: DID, alert_type: Constants.alertTypeId.RAW_Power_Off,
      is_active: recordStatus, created_by
    }
  });

  if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
    transAlertStatusDoc.is_active = 0;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
  } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
    transAlertStatusDoc.is_active = 1;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
    errorFlag = true;
  }

  if (errorFlag || (docStatus && recordStatus)) {
    await creatAlertNotificationToOrg({
      message: `Raw power is off (VE and VN value is 0)`,
      alertType: Constants.alertStatus.UPS,
      DID,
      cc: "tushar.k@cabbagesoft.com",
      columnValue: `1`,
      rxtype: ['internal'],
      severity: 'High',
      priority: 'High',
      istdate,
      rawDataID
    })
  }
}

const upsAbove_235Volt = async ({ rawDataID, istdate, VU, DID }) => {
  //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
  const created_by = `upsAbove_235Volt_`
  let errorFlag = false
  let recordStatus = VU > 235 ? 1 : 0
  const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
    where: { DID: DID, alert_type: Constants.alertTypeId.UPS_Above_235_Volt },
    defaults: {
      DID: DID, alert_type: Constants.alertTypeId.UPS_Above_235_Volt,
      is_active: recordStatus, created_by
    }
  });

  if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
    transAlertStatusDoc.is_active = 0;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
  } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
    transAlertStatusDoc.is_active = 1;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
    errorFlag = true;
  }

  if (errorFlag || (docStatus && recordStatus)) {
    await creatAlertNotificationToOrg({
      message: `UPS output is ${VU}V above set 235V`,
      alertType: Constants.alertStatus.UPS,
      cc: "tushar.k@cabbagesoft.com",
      DID,
      columnValue: `1`,
      rxtype: ['internal'],
      severity: 'High',
      priority: 'High',
      istdate,
      rawDataID
    })
  }
}

const upsEarth = async ({ rawDataID, istdate, DIFF, DID }) => {
  //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
  winston.info("Alerts for Ups Earthing")
  const created_by = `upsEarth_`
  let errorFlag = false
  let recordStatus = DIFF > 2 ? 1 : 0
  const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
    where: { DID: DID, alert_type: Constants.alertTypeId.UPS_Earthing_Diff },
    defaults: {
      DID: DID, alert_type: Constants.alertTypeId.UPS_Earthing_Diff,
      is_active: recordStatus, created_by
    }
  });

  if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
    transAlertStatusDoc.is_active = 0;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
  } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
    transAlertStatusDoc.is_active = 1;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
    errorFlag = true;
  }

  if (errorFlag || (docStatus && recordStatus)) {
    await creatAlertNotificationToOrg({
      message: `UPS Earthing Difference ${DIFF} is  greater Than 2`,
      alertType: Constants.alertStatus.UPS,
      cc: "tushar.k@cabbagesoft.com",
      DID,
      columnValue: `1`,
      rxtype: ['internal'],
      severity: 'High',
      priority: 'High',
      istdate,
      rawDataID
    })
  }
}

const rawPowerRestore = async ({ rawDataID, istdate, VN, VE, DID }) => {
  //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
  const created_by = `rawPowerRestore_`
  let errorFlag = false
  let recordStatus = (VN && VE) > 0 ? 1 : 0
  const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
    where: { DID: DID, alert_type: Constants.alertTypeId.RawPower_Restore },
    defaults: {
      DID: DID, alert_type: Constants.alertTypeId.RawPower_Restore,
      is_active: recordStatus, created_by
    }
  });

  if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
    transAlertStatusDoc.is_active = 0;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
  } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
    transAlertStatusDoc.is_active = 1;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
    errorFlag = true;
  }

  if (errorFlag || (docStatus && recordStatus)) {
    await creatAlertNotificationToOrg({
      message: `Raw Power VN:${VN} and VE:${VE} is now Restore `,
      alertType: Constants.alertStatus.UPS,
      DID,
      columnValue: `1`,
      rxtype: ['internal'],
      severity: 'High',
      priority: 'High',
      istdate,
      rawDataID
    })
  }
}

const batteryVoltStatus = async ({ rawDataID, istdate, VB, DID }) => {
  //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
  const created_by = `batteryVoltStatus_`
  let errorFlag = false
  let recordStatus = VB < 10.2 ? 1 : 0
  const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
    where: { DID: DID, alert_type: Constants.alertTypeId.BATTERY_Voltage_Status },
    defaults: {
      DID: DID, alert_type: Constants.alertTypeId.BATTERY_Voltage_Status,
      is_active: recordStatus, created_by
    }
  });

  if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
    transAlertStatusDoc.is_active = 0;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
  } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
    transAlertStatusDoc.is_active = 1;
    transAlertStatusDoc.updated_by = created_by;
    await transAlertStatusDoc.save();
    errorFlag = true;
  }

  if (errorFlag || (docStatus && recordStatus)) {
    await creatAlertNotificationToOrg({
      message: `Battery Voltage ${VB}V is less than 10.2V`,
      alertType: Constants.alertStatus.UPS,
      DID,
      columnValue: VB,
      rxtype: ['internal'],
      severity: 'High',
      priority: 'High',
      istdate,
      rawDataID
    })
  }
}

// const signageCheckAndSendAlert = async ({ value, istdate, DID }) => {
//   try {
//     let newDID = DID
//     if (DID.charAt(DID.length - 1) == "A" || DID.charAt(DID.length - 1) == "S")
//       newDID = DID.split('').splice(0, DID.length - 1).join('')

//     const alertType = Constants.alertTypeId.Signage
//     const created_by = `signageCheckAndSendAlert_`
//     try {
//       //TODO: set sunrise sunset time from city which can come from mst_loction
//       const cityData = await db.sequelize.query(`select sun_rise,sun_set,mc.city_id from mst_city mc,mst_location ml 
//                                                   where mc.city_id=ml.city_id and ml.dev_id="${newDID}"`,
//         { type: db.sequelize.QueryTypes.SELECT });

//       let istdateMo = 0

//       if (cityData[0] && cityData[0].sun_rise && cityData[0].sun_set) {
//         istdateMo = moment(istdate).isBetween(moment(cityData[0].sun_rise, 'HH:mm A'), moment(cityData[0].sun_set, 'HH:mm A'))
//       } else {
//         istdateMo = moment(istdate).isBetween(moment('06:00:00', 'HH:mm:ss'), moment('18:00:00', 'HH:mm:ss'))
//       }
//       // const istdateMo = moment(istdate).isBetween(moment(cityData[0].sun_rise, 'HH:mm:ss'), moment(cityData[0].sun_set, 'HH:mm:ss'))

//       // const istdateMo = moment(istdate).isBetween(moment('06:00:00', 'HH:mm:ss'), moment('18:00:00', 'HH:mm:ss'))
//       const columns = [];
//       // after working condi
//       if (value == 1 && istdateMo) {
//         columns.push(`Signage is ON in Day Time`)
//       }

//       // in working not SIGN_status problem condi
//       if (value != 1 && !istdateMo) {
//         columns.push(`Signage is OFF in Night Time`)
//       }

//       let errorFlag = false;
//       const recordStatus = columns.length ? 1 : 0
//       const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
//         where: { DID: DID, alert_type: alertType },
//         defaults: {
//           DID: DID, alert_type: alertType,
//           is_active: recordStatus, created_by
//         }
//       });

//       if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
//         transAlertStatusDoc.is_active = 0;
//         transAlertStatusDoc.updated_by = created_by;
//         await transAlertStatusDoc.save();
//       } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
//         transAlertStatusDoc.is_active = 1;
//         transAlertStatusDoc.updated_by = created_by;
//         await transAlertStatusDoc.save();
//         errorFlag = true;
//       }

//       if (errorFlag || (docStatus && recordStatus)) {
//         await creatAlertNotificationToOrg({
//           message: `${columns.join(',')}`,
//           alertType: Constants.alertStatus.Signage,
//           columnValue: value,
//           DID, rxtype: ['internal', 'busi'],
//           severity: 'Minor', priority: 'Low',
//         })
//       }
//     } catch (error) {
//       winston.info(`Cron signageCheckAndSendAlert error----->", ${error.stack}`);
//     }
//   } catch (err) {
//     winston.info(`Cron signageCheckAndSendAlert error----->", ${err.stack}`);
//     console.error(err)
//   }
// };

const signageCheckAndSendAlert = async ({ rawDataID, value, istdate, DID, cs, loc_id = '' }) => {
  try {
    let newDID = DID
    if (DID.charAt(DID.length - 1) == "A" || DID.charAt(DID.length - 1) == "S")
      newDID = DID.split('').splice(0, DID.length - 1).join('')

    const alertType = Constants.alertTypeId.Signage
    const created_by = `signageCheckAndSendAlert_`
    try {
      //TODO: set sunrise sunset time from city which can come from mst_loction
      const cityData = await db.sequelize.query(`select sun_rise,sun_set,mc.city_id from mst_city mc,mst_location ml 
                                                  where mc.city_id=ml.city_id and ml.dev_id="${newDID}"`,
        { type: db.sequelize.QueryTypes.SELECT });

      let istdateMo = 0

      if (cityData[0] && cityData[0].sun_rise && cityData[0].sun_set) {
        istdateMo = moment(istdate).isBetween(moment(cityData[0].sun_rise, 'HH:mm A'), moment(cityData[0].sun_set, 'HH:mm A'))
      } else {
        istdateMo = moment(istdate).isBetween(moment('06:00:00', 'HH:mm:ss'), moment('18:00:00', 'HH:mm:ss'))
      }
      // const istdateMo = moment(istdate).isBetween(moment(cityData[0].sun_rise, 'HH:mm:ss'), moment(cityData[0].sun_set, 'HH:mm:ss'))

      // const istdateMo = moment(istdate).isBetween(moment('06:00:00', 'HH:mm:ss'), moment('18:00:00', 'HH:mm:ss'))
      const columns = [];
      // after working condi


      // in working not SIGN_status problem condi
      if (value == 1 && !istdateMo && cs == 0) {
        columns.push(`Signage is not working `)
      }
      if (value == 0 && !istdateMo) {
        columns.push(`Signage Device Error `)
      }

      let errorFlag = false;
      const recordStatus = columns.length ? 1 : 0
      const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
        where: { DID: DID, alert_type: alertType },
        defaults: {
          DID: DID, alert_type: alertType,
          is_active: recordStatus, created_by
        }
      });

      if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
        transAlertStatusDoc.is_active = 0;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
        if (loc_id) await siteAlert.update({ is_active: 3 }, { where: { alert_type: alertType, location: loc_id, severity: Constants.priorityStatus.Minor, priority: Constants.severityStatus.Low } })
      } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
        transAlertStatusDoc.is_active = 1;
        transAlertStatusDoc.updated_by = created_by;
        await transAlertStatusDoc.save();
        errorFlag = true;
      }

      if (errorFlag || (docStatus && recordStatus)) {
        await creatAlertNotificationToOrg({
          message: `${columns.join(',')}`,
          alertType: Constants.alertStatus.Signage,
          columnValue: value,
          DID, rxtype: ['internal', 'busi'],
          severity: 'Minor', priority: 'Low',
          istdate,
          rawDataID,
          alertTypeId: alertType
        })
      }
    } catch (error) {
      winston.info(`Cron signageCheckAndSendAlert error----->", ${error.stack}`);
    }
  } catch (err) {
    winston.info(`Cron signageCheckAndSendAlert error----->", ${err.stack}`);
    console.error(err)
  }
};

//This is not in use now
// const ACRelayStatus = async ({rawDataID,istdate, Type, Status, caValue, TMValue, DID }) => {
//   //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
//   let recordStatus = 0
//   const created_by = `acStatus_`
//   let errorFlag = false
//   if (Status) {
//     if (Status && caValue >= Constants.CA_value) {
//       recordStatus = 0
//     }
//     if (Status && (caValue < Constants.CA_value || TMValue > 30)) {
//       recordStatus = 1
//     }
//     let alertType = Type == 1 ? Constants.alertTypeId.AC1_STATUS : Constants.alertTypeId.AC2_STATUS
//     let acType = Type == 1 ? "AC1" : "AC2";
//     const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
//       where: { DID: DID, alert_type: alertType },
//       defaults: {
//         DID: DID, alert_type: alertType,
//         is_active: recordStatus, created_by
//       }
//     });

//     if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
//       transAlertStatusDoc.is_active = 0;
//       transAlertStatusDoc.updated_by = created_by;
//       await transAlertStatusDoc.save();
//     } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
//       transAlertStatusDoc.is_active = 1;
//       transAlertStatusDoc.updated_by = created_by;
//       await transAlertStatusDoc.save();
//       errorFlag = true;
//     }

//     if (errorFlag || (docStatus && recordStatus)) {
//       await creatAlertNotificationToOrg({
//         message: `${acType} Status is Not Working`,
//         alertType: Constants.alertStatus.AC,
//         DID,
//         columnValue: `AC1:${Status},TM:${TMValue},caOne:${caValue}`,
//         rxtype: ['internal'],
//         severity: 'High',
//         priority: 'High',
//         istdate,
//         rawDataID
//       })
//     }
//   }
// }

const ACRelayStatus = async ({ rawDataID, istdate, Type, Status, caValue, TMValue, DID }) => {
  //AC1_status && (caOneValue < Constants.CA_value || TMValue > 30
  let recordStatus = 0
  const created_by = `acStatus_`
  let errorFlag = false
  if (Status) {
    if (Status && caValue > 0) {
      recordStatus = 0
    }
    if (Status && (caValue == 0 && TMValue > 35)) {
      recordStatus = 1
    }
    let alertType = Type == 1 ? Constants.alertTypeId.AC1_STATUS : Constants.alertTypeId.AC2_STATUS
    let acType = Type == 1 ? "AC1" : "AC2";
    const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
      where: { DID: DID, alert_type: alertType },
      defaults: {
        DID: DID, alert_type: alertType,
        is_active: recordStatus, created_by
      }
    });

    if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
      transAlertStatusDoc.is_active = 0;
      transAlertStatusDoc.updated_by = created_by;
      await transAlertStatusDoc.save();
    } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
      transAlertStatusDoc.is_active = 1;
      transAlertStatusDoc.updated_by = created_by;
      await transAlertStatusDoc.save();
      errorFlag = true;
    }

    if (errorFlag || (docStatus && recordStatus)) {
      await creatAlertNotificationToOrg({
        message: `${acType} is Not Working`,
        alertType: Constants.alertStatus.AC,
        cc: "tushar.k@cabbagesoft.com",
        DID,
        columnValue: `AC1:${Status},TM:${TMValue},caOne:${caValue}`,
        rxtype: ['internal'],
        severity: 'High',
        priority: 'High',
        istdate,
        rawDataID
      })
    }
  }
}

//This is not in use now
// const SignageStatus = async ({ SIGN_status, CLValue, DID }) => {
//   if (SIGN_status) {
//     let errorFlag = false
//     const created_by = `signageStatus_`
//     let recordStatus = CLValue < 0.5 ? 1 : 0
//     let alertType = Constants.alertTypeId.SIGNAGE_STATUS;
//     const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
//       where: { DID: DID, alert_type: alertType },
//       defaults: {
//         DID: DID, alert_type: alertType,
//         is_active: recordStatus, created_by
//       }
//     });

//     if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
//       transAlertStatusDoc.is_active = 0;
//       transAlertStatusDoc.updated_by = created_by;
//       await transAlertStatusDoc.save();
//     } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
//       transAlertStatusDoc.is_active = 1;
//       transAlertStatusDoc.updated_by = created_by;
//       await transAlertStatusDoc.save();
//       errorFlag = true;
//     }

//     if (errorFlag || (docStatus && recordStatus)) {
//       await creatAlertNotificationToOrg({
//         message: `sinage status is off`,
//         alertType: Constants.alertStatus.Signage,
//         DID,
//         columnValue: `SIGN:${SIGN_status},CLValue:${CLValue}`,
//         rxtype: ['internal'],
//         severity: 'High',
//         priority: 'High',
//       })
//     }
//   }
// }


allcron.notificationCron = new cron('0 */2 * * * *', async () => {
  // allcron.notificationCron = new cron('*/1 * * * *', async () => {
  let data = await systemcontrol.findOne({ where: { cron_name: 'notificationCron', run_cron_flag: 1, cronStatus: true } })
  if (data) {

    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "notificationCron", status: "started", parent_table: "notification" });
    let created_by = "notificationCron" + cronEntry.id;
    try {
      const startTime = new Date()
      winston.info(`notificationCron start at ${new Date()}`)
      await NotificationsService.sendNotification({});
      winston.info(`notificationCron  end at ${new Date()} started at ${startTime}`)
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      winston.error(`notificationCron failed with error: ${JSON.stringify(err)}`)
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    } finally {
      systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
    }
  } else {
    winston.info("Cron notificationCron error----->", "Cron flag set to 0 or cronStatus is false");
  }
})

// allcron.processRawData = new cron("*/1 * * * * *", async function () {
const checkNumber = (v) => {
  v = v.replace(/[^0-9.]/g, "")
  if (v.indexOf('.') != -1) {
    return Math.round((v.split(".")[0].slice(0, 4) + "." + v.split(".")[1].slice(0, 3)) * 1000) / 1000 || 0
  }
  return parseInt(v.slice(0, 4)) || 0
}
const euronetData = async function (data, rawDataID, created_by) {
  let decodedData = data

  let evt_dt1 = "";
  let evt_dt = null;
  let TI;
  let UE = ""
  // if (decodedData[31] && decodedData[31].split(":")[1]) {
  if (decodedData[0].split(":")[1] == "3.0" || decodedData[0].split(":")[1] == "3.1") {
    evt_dt1 = decodedData[29].split(":")[1]
    let hh = evt_dt1.substring(0, 2);
    let mm = evt_dt1.substring(2, 4);
    let ss = evt_dt1.substring(4, 6);
    let dd = evt_dt1.substring(6, 8);
    let mon = evt_dt1.substring(8, 10);
    let yy = evt_dt1.substring(10, 12);
    evt_dt = new Date('20' + yy + '-' + mon + '-' + dd + ' ' + hh + ':' + mm + ':' + ss);
    var istdate = evt_dt;
    TI = decodedData[29].split(":")[1]
  }
  else if (decodedData[0].split(":")[1] == "4.1") {
    UE = parseFloat(decodedData[26].split(":")[1]) // upsNetural

    evt_dt1 = decodedData[30].split(":")[1]
    let hh = evt_dt1.substring(0, 2);
    let mm = evt_dt1.substring(2, 4);
    let ss = evt_dt1.substring(4, 6);
    let dd = evt_dt1.substring(6, 8);
    let mon = evt_dt1.substring(8, 10);
    let yy = evt_dt1.substring(10, 12);
    evt_dt = new Date('20' + yy + '-' + mon + '-' + dd + ' ' + hh + ':' + mm + ':' + ss);
    var istdate = evt_dt;
    TI = decodedData[30].split(":")[1]
    decodedData.splice(26, 1)
  }
  decodedData.shift();

  // evt_dt1 = decodedData[30].split(":")[1]
  // let hh = evt_dt1.substring(6, 8);
  // let mm = evt_dt1.substring(8, 10);
  // let ss = evt_dt1.substring(10, 12);
  // let dd = evt_dt1.substring(4, 6);
  // let mon = evt_dt1.substring(2, 4);
  // let yy = evt_dt1.substring(0, 2);
  // evt_dt = new Date('20' + yy + '-' + mon + '-' + dd + ' ' + hh + ':' + mm + ':' + ss);
  // var istdate = new Date(evt_dt.getTime() + (5 * 60 + 30) * 60000);

  if (!moment(evt_dt, "YYYY-MM-DD HH:mm:ss", true).isValid() || !(moment(evt_dt).format("YYYY") < "2024")) {
    throw "Invalid Date"
  }

  // }

  if (check_KWH_ValueIs0(decodedData)) throw "Invalid KWH values for (HA1|HA2|HL|HS) is comming 0"

  let AC1_status = 0;
  let AC2_status = 0;
  let LOBBY_status = 0;
  let SIGN_status = 0;
  let MODEM_status = 0
  let MODE_status = 0
  const DID = decodedData[0] ? decodedData[0].split(":")[1] : null
  let totalBattertVolt = await device.findOne({ where: { dev_id: DID, is_active: Constants.active }, attributes: ["total_voltage"] })
  totalBattertVolt = (totalBattertVolt && totalBattertVolt.dataValues.total_voltage) || null
  let VN = decodedData[22] ? decodedData[22].split(":")[1] : null  //phaseToNeutralVolt

  let VE = decodedData[23].split(":")[1]    //phaseToEarthVolt
  if (VN < 50) {
    VN = 0
    VE = 0
  }

  const phaseVoltageDiff = Math.abs(VN - VE)
  let phaseVoltageStatus = phaseVoltageDiff <= 10 ? Constants.colorStatus.green : Constants.colorStatus.red  // VoltageStatus
  if ((VN || VE) == 0) phaseVoltageStatus = Constants.colorStatus.red

  const VB = decodedData[25].split(":")[1]
  const batteryStatus = VB <= totalBattertVolt * 0.02 ? Constants.colorStatus.red : Constants.colorStatus.green;
  let charging = -1
  let discharging = -1
  if (totalBattertVolt) {
    RoundValue = VB / totalBattertVolt * 100
    charging = Math.round(RoundValue * 100) / 100
    discharging = (100 - charging) || 0

  }
  const HUC = decodedData[8] ? decodedData[8].split(":")[1] : null
  const HD = Math.round(decodedData[6].split(":")[1] * 1000) / 1000 || null;
  const CD = Math.round(decodedData[15].split(":")[1] * 1000) / 1000 || null;
  const dvrStatus = CD > 0 ? Constants.colorStatus.green : Constants.colorStatus.red
  //const CD = decodedData[15].split(":")[1]
  // const currentDvrStatus = CD > 0 ? Constants.colorStatus.green : Constants.colorStatus.red
  //TODO:set Vu to 2 
  const VU = parseFloat(decodedData[24].split(":")[1]) // upsNetural
  const upsPhaseToEarthVolt = UE || parseFloat(phaseVoltageDiff) + VU    //upsPhaseToEarthVolt
  const upsPhaseVoltageDiff = Math.abs(VU - parseFloat(upsPhaseToEarthVolt))    // upsVoltageDiff
  let upsPhaseVoltageStatus = upsPhaseVoltageDiff <= 2 ? Constants.colorStatus.green : Constants.colorStatus.red

  if (VU == 0) upsPhaseVoltageStatus = Constants.colorStatus.red


  const relayValue = decodedData[27] ? decodedData[27].split(":")[1].split("") : null;

  const caOneValue = parseFloat(decodedData[11] ? decodedData[11].split(":")[1] : 0);
  const caTwoValue = parseFloat(decodedData[12] ? decodedData[12].split(":")[1] : 0);
  const CLValue = parseFloat(decodedData[14] ? decodedData[14].split(":")[1] : 0);
  let TMValue = parseFloat(decodedData[18] ? decodedData[18].split(":")[1] : 0);


  if (relayValue[0] == "1") {
    AC1_status = 1
  }
  if (relayValue[1] == "1") {
    AC2_status = 1
  }
  if (relayValue[2] == "1") {
    LOBBY_status = 1
  }
  if (relayValue[3] == "1") {
    SIGN_status = 1
  }
  if (relayValue[4] == "1") {
    MODEM_status = 1
  }
  if (relayValue[7] == "1") {
    MODE_status = 1
  }
  // console.log("time 1 " + (Date.now() - start))
  let locDetails;
  let IAQ
  try {
    // const dev_id = DID.split('').splice(0, DID.length - 1).join('')
    const dev_id = DID
    // let updateLocationMst = false;
    let locationUpdateKeys = ``;
    const locationUpdateData = {};
    if (dev_id) {

      // }
      locDetails = await db.sequelize.query(
        `SELECT loc_id,city_name,IAQ,signage_status FROM mst_location ml,mst_city mc  where ml.city_id=mc.city_id and dev_id=:dev_id and ml.is_active=1 `,
        { type: db.sequelize.QueryTypes.SELECT, replacements: { dev_id } }
      );
      IAQ = (locDetails[0] && locDetails[0].IAQ) || null
      if (locDetails && locDetails[0] && locDetails[0].signage_status !== SIGN_status) {
        let locationUpdateKeys = ''
        let updateKeys = {}
        locationUpdateKeys += `dev_status_last_recived=:dev_status_last_recived,`
        updateKeys.dev_status_last_recived = evt_dt
        updateKeys.dev_id = dev_id


        if (SIGN_status) {
          updateKeys.signage_st_time = evt_dt,
            locationUpdateKeys += `signage_st_time=:signage_st_time,`
        } else {
          updateKeys.signage_end_time = evt_dt,
            locationUpdateKeys += `signage_end_time=:signage_end_time,`
        }
        SIGN_status ? updateKeys.signage_st_time = evt_dt : updateKeys.signage_end_time = evt_dt;
        locationUpdateKeys += 'signage_status =:signage_status'
        updateKeys.signage_status = SIGN_status,
          await db.sequelize.query(
            `UPDATE  mst_location  SET ${locationUpdateKeys} where dev_id=:dev_id `,
            { type: db.sequelize.QueryTypes.UPDATE, replacements: updateKeys }
          );

        // SIGN_status ? query += `signage_st_time = ${evt_dt}, ` : query += `signage_end_time= ${evt_dt},`;
        // query += `signage_status = ${SIGN_status}`
        // query += ` where dev_id="${dev_id}"`

        // locationUpdateData.dev_id = dev_id;
      }
    }
  } catch (error) {
    console.error(error)
  }

  let duration = 0
  let powerOn = 0;
  let powerOff = 0;
  let status = 0
  if (VE < 10) {
    if (HUC > 0.1) {
      powerOff = 1
    }
  }
  if (VE >= 10) {
    powerOn = 1
    status = 1
  }
  // fetch data or create
  const upsLogsData = await db.sequelize.query(
    `SELECT * FROM ups_logs where device_id=:device_id order by 1 desc limit 1`,
    { type: db.sequelize.QueryTypes.SELECT, replacements: { device_id: DID } }
  );
  let locId = (locDetails[0] && locDetails[0].loc_id) || null
  let cityName = (locDetails[0] && locDetails[0].city_name) || null
  if (upsLogsData.length) {
    if (powerOff && upsLogsData[0].status == 1) {
      //create
      powerOff = powerOff ? evt_dt : null
      powerOn = powerOn ? evt_dt : null
      await upsLogs.create({ device_id: DID, city_name: cityName, location_id: locId, power_off: powerOff, power_on: powerOn, status })
    }
    if (powerOn && upsLogsData[0].status == 0) {
      //update
      let dif = new Date(evt_dt) - new Date(upsLogsData[0].power_off) || null
      duration = Math.round((dif / 1000) / 60)
      await upsLogs.update({ status: 1, power_on: evt_dt, location_id: locId, duration }, { where: { id: upsLogsData[0].id } })
    }
  } else {
    if (powerOff) await upsLogs.create({ device_id: DID, city_name: cityName, location_id: locId, power_off: evt_dt, status })
  }

  // 1 = green,2 = yellow, 3 = red
  // 1 = connected,  0 = disconnected
  // 1 =  automatic, 0 = manual
  let ac1_status = 2;
  let ac2_status = 2;
  let lobby_status = 2;
  let signage_status = 2;
  let ac2_conn = 2;
  let ac1_conn = 2;
  let ac2_comp = 2;
  let ac1_comp = 2;
  let ac2_cooling = 2;
  let ac1_cooling = 2;
  let ac2_mode = 2;
  let ac1_mode = 2;

  if (AC1_status && caOneValue > 0) {
    ac1_status = 1;
    ac1_conn = 1;
  } else if (AC1_status && (caOneValue == 0 && TMValue > 35)) {
    ac1_status = 3;
    ac1_conn = 3;
  }

  await ACRelayStatus({ rawDataID, istdate, Type: 1, Status: AC1_status, caValue: caOneValue, TMValue, DID, created_by })


  if (AC2_status && caTwoValue > 0) {
    ac2_status = 1;
    ac2_conn = 1;
  } else if (AC2_status && (caTwoValue == 0 && TMValue > 35)) {
    ac2_status = 3;
    ac2_conn = 3;

  }

  await ACRelayStatus({ rawDataID, istdate, Type: 2, Status: AC2_status, caValue: caTwoValue, TMValue, DID, created_by })

  // await SignageStatus({ SIGN_status, CLValue, DID })


  if (LOBBY_status) {
    lobby_status = 1
  }

  //TODO: set sunrise sunset time from city which can come from mst_loction
  const cityData = await db.sequelize.query(`select sun_rise,sun_set,mc.city_id from mst_city mc,mst_location ml 
                                                where mc.city_id=ml.city_id and ml.dev_id="${DID}"`,
    { type: db.sequelize.QueryTypes.SELECT });

  let istdateMo = 0

  if (cityData[0] && cityData[0].sun_rise && cityData[0].sun_set) {
    istdateMo = moment(istdate).isBetween(moment(cityData[0].sun_rise, 'HH:mm A'), moment(cityData[0].sun_set, 'HH:mm A'))
  } else {
    istdateMo = moment(istdate).isBetween(moment('06:00:00', 'HH:mm:ss'), moment('18:00:00', 'HH:mm:ss'))
  }

  const DeviceData = await device.findOne({ where: { dev_id: DID }, attributes: ['is_signage'] })
  const cs = decodedData[14].split(":")[1]
  if (DeviceData && DeviceData.is_signage == 1) {
    if (SIGN_status == 1 && !istdateMo && cs > 0) { //Signage green status
      signage_status = 1
    }

    //
    if (SIGN_status == 1 && !istdateMo && cs == 0) { //Signage Red Status 
      signage_status = 3
    }
    if (SIGN_status == 0 && !istdateMo) {
      signage_status = 3
    }

    if (SIGN_status == 0 && istdateMo) {
      signage_status = 2
    }

    if (evt_dt) {
      // checkDeviceSignageStatus({ DID, SIGN_status, evt_dt })
      await signageCheckAndSendAlert({ rawDataID, istdate, DID, value: SIGN_status, istdate: evt_dt, cs, loc_id: locDetails[0] && locDetails[0].loc_id })
    }
  }





  if (AC2_status && TMValue > 30) {
    ac2_comp = 1
  }
  if (AC1_status && TMValue > 30) {
    ac1_comp = 1
  }


  if (ac2_comp == 1 && TMValue < 30) {
    ac2_cooling = 1
  }
  if (ac1_comp == 1 && TMValue < 30) {
    ac1_cooling = 1
  }

  upsPower_off({ rawDataID, istdate, VU, DID })
  rawPower_off({ rawDataID, istdate, VN, VE, DID })
  upsAbove_235Volt({ rawDataID, istdate, VN, VE, DID })
  // upsEarth({rawDataID,istdate, DIFF: upsPhaseVoltageDiff, DID })
  rawPowerRestore({ rawDataID, istdate, VN, VE, DID })
  acLoadConsumption({ rawDataID, istdate, Type: 1, Status: AC1_status, caValue: caOneValue, DID, loc_id: locDetails[0] && locDetails[0].loc_id })
  acLoadConsumption({ rawDataID, istdate, Type: 2, Status: AC2_status, caValue: caTwoValue, DID, loc_id: locDetails[0] && locDetails[0].loc_id })
  batteryVoltStatus({ rawDataID, istdate, VB, DID })


  euronet_tempCheckAndSendAlert({ rawDataID, istdate, TMValue, DID, loc_id: locDetails[0] && locDetails[0].loc_id });
  //TODO : this jugaad for now
  if (TMValue < 18 || TMValue > 60)
    TMValue = 24;




  let tempAvgData = 0;
  let deviceIdExists = false;

  if (tempDevID.includes(DID)) {
    let avgData = {};
    for (let i = global.deviceAvgData.length - 1; i >= 0; i--)
      if (global.deviceAvgData[i].device_id == DID) {
        avgData = global.deviceAvgData[i];
        break;
      }
    tempAvgData = avgData.avg || null
    tempAvgData = (tempAvgData + TMValue) / 2
    avgData.avg = tempAvgData


    deviceIdExists = true;
  }
  if (!deviceIdExists) {
    const avgData = await db.sequelize.query(`SELECT if(AVG(TM_avg) < 18, 24, AVG(TM_avg)  )as TMAvg
            FROM trans_hourly_data WHERE DID= :deviceId
            AND evt_dt >= date_sub(now(), interval 24 hour) and YEAR(now())=YEAR(evt_dt) limit 720`, {
      type: db.sequelize.QueryTypes.SELECT,
      replacements: { istdate: istdate, deviceId: DID }
    })
    if (avgData.length && avgData[0].TMAvg != null) {
      global.deviceAvgData.push({ device_id: DID, avg: avgData[0].TMAvg })
      tempDevID.push(DID)
      tempAvgData = avgData[0].TMAvg
    }
  }

  expandedDataObj.push({
    raw_data_id: rawDataID,
    phase_voltage_diff: phaseVoltageDiff,
    phase_voltage_status: phaseVoltageStatus,
    battery_status: batteryStatus,
    // dvr_status: dvrStatus,
    ups_voltage_diff: upsPhaseVoltageDiff,
    // total_consumption: 0,
    // ups: decodedData[38].split(":")[1],
    ups_phase_to_earth_voltage: upsPhaseToEarthVolt,
    ups_voltage_diff: upsPhaseVoltageDiff,
    ups_voltage_status: upsPhaseVoltageStatus,
    IAQ,
    charging,
    discharging,
    dvr_status: dvrStatus,
    UE,

    DID: decodedData[0] ? decodedData[0].split(":")[1] : null,
    IM: decodedData[1] ? decodedData[1].split(":")[1] : null,
    HAone: decodedData[2] ? checkNumber(decodedData[2].split(":")[1]) : null,
    HAtwo: decodedData[3] ? checkNumber(decodedData[3].split(":")[1]) : null,
    HS: decodedData[4] ? checkNumber(decodedData[4].split(":")[1]) : null,
    HL: decodedData[5] ? checkNumber(decodedData[5].split(":")[1]) : null,
    HD: decodedData[6] ? checkNumber(decodedData[6].split(":")[1]) : null,
    HUO: decodedData[7] ? checkNumber(decodedData[7].split(":")[1]) : null,
    HUC: decodedData[8] ? checkNumber(decodedData[8].split(":")[1]) : null,
    oneH: decodedData[9] ? checkNumber(decodedData[9].split(":")[1]) : null,
    twoH: decodedData[10] ? checkNumber(decodedData[10].split(":")[1]) : null,
    CAone: decodedData[11] ? checkNumber(decodedData[11].split(":")[1]) : 0,
    CAtwo: decodedData[12] ? checkNumber(decodedData[12].split(":")[1]) : 0,
    CL: decodedData[13] ? checkNumber(decodedData[13].split(":")[1]) : null,
    CS: decodedData[14] ? checkNumber(decodedData[14].split(":")[1]) : null,
    CD: decodedData[15] ? checkNumber(decodedData[15].split(":")[1]) : null,
    CUO: decodedData[16] ? checkNumber(decodedData[16].split(":")[1]) : null,
    CUC: decodedData[17] ? checkNumber(decodedData[17].split(":")[1]) : null,
    //            TM: decodedData[18] ? decodedData[18].split(":")[1] : null,
    TM: TMValue,
    HM: decodedData[19] ? checkNumber(decodedData[19].split(":")[1]) : null,
    PR: decodedData[20] ? decodedData[20].split(":")[1] : null,
    DR: decodedData[21] ? decodedData[21].split(":")[1] : null,
    VB,
    VN,
    VE,
    VU,
    RS: decodedData[27] ? decodedData[27].split(":")[1] : null,
    TI,
    evt_dt: istdate,
    ac1_status,
    ac2_status,
    signage_status,
    lobby_status: LOBBY_status, // Extra field
    modem_status: MODEM_status,  // Extra field
    device_mode: MODE_status, // Extra field
    ac2_conn,
    ac1_conn,
    ac2_comp,
    ac1_comp,
    ac2_cooling,
    ac1_cooling,
    ac2_mode,
    ac1_mode,
    avg_tmp: tempAvgData?.toFixed(2) || null,
    AS: 0,
    is_active: Constants.active, created_by: created_by
  })
}

const icontrolData = async function (data, rawDataID, created_by) {
  //Validation 
  // 1) no of parameters expected minimum
  // 2) each parameter need to have 2 values seperated by colun 
  //3) data validation
  //4) data type validation
  //5) data length validation


  const decodedData = data
  // let evt_dt1 = "";
  let evt_dt = null;
  // if (decodedData[26].split(":")[1] && decodedData[26].split(":")[1].length == 12) {
  let evt_dt1 = '';
  evt_dt1 = decodedData[26].split(":")[1];
  //console.log("evt_dt" + evt_dt1);
  let hh = evt_dt1.substring(0, 2);
  let mm = evt_dt1.substring(2, 4);
  let ss = evt_dt1.substring(4, 6);
  let dd = evt_dt1.substring(6, 8);
  let mon = evt_dt1.substring(8, 10);
  let yy = evt_dt1.substring(10, 12);
  evt_dt = new Date('20' + yy + '-' + mon + '-' + dd + ' ' + hh + ':' + mm + ':' + ss);
  //console.log("evt_dt" + evt_dt);
  var istdate = evt_dt;

  if (!moment(evt_dt, "YYYY-MM-DD HH:mm:ss", true).isValid() || !(moment(evt_dt).format("YYYY") < "2024")) {

    throw "Invalid Date"
  }
  // var istdate = new Date(evt_dt.getTime() + (5 * 60 + 30) * 60000);
  //console.log("istdate" + istdate);
  // }


  let AC1_status = 0;
  let AC2_status = 0;
  let LOBBY_status = 0;
  let SIGN_status = 0;
  const DID = decodedData[0] ? decodedData[0].split(":")[1] : null

  const relayValue = parseInt(decodedData[25] ? decodedData[25].split(":")[1] : 2);
  // const relayValue = parseInt(decodedData[25] ? decodedData[25].split("") : null);

  const caOneValue = parseFloat(decodedData[11] ? decodedData[11].split(":")[1] : 0);
  const caTwoValue = parseFloat(decodedData[12] ? decodedData[12].split(":")[1] : 0);
  const CLValue = parseFloat(decodedData[14] ? decodedData[14].split(":")[1] : 0);
  let TMValue = parseFloat(decodedData[18] ? decodedData[18].split(":")[1] : 0);


  switch (relayValue) {
    case Constants.relayComb.AC2:
      AC2_status = 1
      break;
    case Constants.relayComb.AC1:
      AC1_status = 1
      break;
    case Constants.relayComb.LOBBY:
      LOBBY_status = 1
      break;
    case Constants.relayComb.SIGN:
      SIGN_status = 1
      break;
    case Constants.relayComb.AC1_AC2:
      AC2_status = 1
      AC1_status = 1
      break;
    case Constants.relayComb.AC2_LOBBY:
      AC2_status = 1
      LOBBY_status = 1
      break;
    case Constants.relayComb.AC2_SIGN:
      AC2_status = 1
      SIGN_status = 1
      break;
    case Constants.relayComb.AC1_LOBBY:
      AC1_status = 1
      LOBBY_status = 1
      break;
    case Constants.relayComb.AC1_SIGN:
      AC1_status = 1
      SIGN_status = 1
      break;
    case Constants.relayComb.AC2_AC1_LOBBY:
      AC1_status = 1
      AC2_status = 1
      LOBBY_status = 1
      break;
    case Constants.relayComb.AC2_AC1_SIGN:
      AC1_status = 1
      AC2_status = 1
      SIGN_status = 1
      break;
    case Constants.relayComb.LOBBY_SIGN:
      SIGN_status = 1
      LOBBY_status = 1
      break;
    case Constants.relayComb.LOBBY_SIGN_AC2:
      SIGN_status = 1
      LOBBY_status = 1
      AC2_status = 1
      break;
    case Constants.relayComb.LOBBY_SIGN_AC1:
      SIGN_status = 1
      LOBBY_status = 1
      AC1_status = 1
      break;
    case Constants.relayComb.LOBBY_SIGN_AC1_AC2:
      SIGN_status = 1
      LOBBY_status = 1
      AC1_status = 1
      AC2_status = 1
      break;
    default:
      break;
  }

  // console.log("time 1 " + (Date.now() - start))
  let locDetails;
  let IAQ;
  try {
    const dev_id = DID.split('').splice(0, DID.length - 1).join('')
    // const dev_id = DID
    let updateLocationMst = false;
    let locationUpdateKeys = ``;
    const locationUpdateData = {};
    if (dev_id) {
      const suffix = DID.split('').pop().toLowerCase()
      if (['a', 's'].includes(suffix)) {
        locationUpdateData.last_recived = evt_dt;
        locationUpdateKeys += `${suffix}_last_recived=:last_recived`
        updateLocationMst = true
      }
      const locDetails = await db.sequelize.query(
        `SELECT * FROM mst_location where dev_id=:dev_id and is_active=1`,
        { type: db.sequelize.QueryTypes.SELECT, replacements: { dev_id } }
      );
      if (locDetails && locDetails[0] && locDetails[0].signage_status !== SIGN_status) {
        if (locDetails[0].signage == 'ATM' && suffix == 'a') {
          const updateKey = SIGN_status == 0 ? 'signage_end_time' : 'signage_st_time'
          locationUpdateKeys += `${updateLocationMst ? ',' : ''} ${updateKey}=:signage_time, signage_status=:SIGN_status`
          locationUpdateData.signage_time = evt_dt;
          locationUpdateData.SIGN_status = SIGN_status
          updateLocationMst = true
        } else if (locDetails[0].signage !== 'ATM' && (suffix == 's' || suffix == 'e')) {
          const updateKey = SIGN_status == 0 ? 'signage_end_time' : 'signage_st_time'
          locationUpdateKeys += `${updateLocationMst ? ',' : ''} ${updateKey}=:signage_time, signage_status=:SIGN_status`
          locationUpdateData.signage_time = evt_dt;
          locationUpdateData.SIGN_status = SIGN_status
          updateLocationMst = true
        }
      }
      IAQ = (locDetails[0] && locDetails[0].IAQ) || null

      // const updateKey = SIGN_status == 0 ? 'signage_end_time' : 'signage_st_time'
      // locationUpdateKeys += `${updateLocationMst ? ',' : ''} ${updateKey}=:signage_time, signage_status=:SIGN_status`
      // locationUpdateData.signage_time = evt_dt;
      // locationUpdateData.SIGN_status = SIGN_status

      // if (updateLocationMst && locationUpdateKeys && Object.keys(locationUpdateData).length) {
      //   locationUpdateData.dev_id = dev_id;
      //   await db.sequelize.query(
      //     `UPDATE  mst_location  SET ${locationUpdateKeys} where dev_id=:dev_id `,
      //     { type: db.sequelize.QueryTypes.UPDATE, replacements: locationUpdateData }
      //   );
      // }

      if (updateLocationMst && locationUpdateKeys && Object.keys(locationUpdateData).length) {
        locationUpdateData.dev_id = dev_id;
        await db.sequelize.query(
          `UPDATE  mst_location  SET ${locationUpdateKeys} where dev_id=:dev_id `,
          { type: db.sequelize.QueryTypes.UPDATE, replacements: locationUpdateData }
        );
      }
      //`UPDATE  mst_device  SET ${locationUpdateKeys} where dev_id=:dev_id `,
    }
  } catch (error) {
    console.error(error)
  }


  // 1 = green,2 = yellow, 3 = red
  // 1 = connected,  0 = disconnected
  // 1 =  automatic, 0 = manual
  let ac1_status = 2;
  let ac2_status = 2;
  let lobby_status = 2;
  let signage_status = 2;
  let ac2_conn = 2;
  let ac1_conn = 2;
  let ac2_comp = 2;
  let ac1_comp = 2;
  let ac2_cooling = 2;
  let ac1_cooling = 2;
  let ac2_mode = 2;
  let ac1_mode = 2;

  await ACRelayStatus({ rawDataID, istdate, Type: 1, Status: AC1_status, caValue: caOneValue, TMValue, DID, created_by })

  if (AC1_status && caOneValue > 0) {
    ac1_status = 1;
    ac1_conn = 1;
  } else if (AC1_status && (caOneValue == 0 && TMValue > 35)) {
    ac1_status = 3;
    ac1_conn = 3;

  }

  await ACRelayStatus({ rawDataID, istdate, Type: 2, Status: AC2_status, caValue: caTwoValue, TMValue, DID, created_by })


  if (AC2_status && caTwoValue > 0) {
    ac2_status = 1;
    ac2_conn = 1;
  } else if (AC2_status && (caTwoValue == 0 && TMValue > 35)) {
    ac2_status = 3;
    ac2_conn = 3;

  }

  // await SignageStatus({ SIGN_status, CLValue, DID })


  // let errorFlag = false;
  // const recordStatus = 1;
  // let alertType = Constants.alertTypeId.SIGNAGE_STATUS;
  // const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
  //   where: { DID: DID, alert_type: alertType },
  //   defaults: {
  //     DID: DID, alert_type: alertType,
  //     is_active: recordStatus, created_by
  //   }
  // });

  // if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
  //   transAlertStatusDoc.is_active = 0;
  //   transAlertStatusDoc.updated_by = created_by;
  //   await transAlertStatusDoc.save();
  // } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
  //   transAlertStatusDoc.is_active = 1;
  //   transAlertStatusDoc.updated_by = created_by;
  //   await transAlertStatusDoc.save();
  //   errorFlag = true;
  // }

  // if (errorFlag || (docStatus && recordStatus)) {
  //   creatAlertNotificationToOrg({
  //     message: `sinage status is off`,
  //     alertType: Constants.alertStatus.Signage,
  //     DID,
  //     columnValue: `SIGN:${SIGN_status},CLValue:${CLValue}`,
  //     rxtype: ['internal'],
  //     severity: 'High',
  //     priority: 'High',
  //   })
  // }
  // if (!SIGN_status) {
  //   signage_status = 3;
  // }
  if (LOBBY_status) {
    lobby_status = 1
  }

  let newDID = DID
  if (DID.charAt(DID.length - 1) == "A" || DID.charAt(DID.length - 1) == "S")
    newDID = DID.split('').splice(0, DID.length - 1).join('')

  //TODO: set sunrise sunset time from city which can come from mst_loction
  const cityData = await db.sequelize.query(`select sun_rise,sun_set,mc.city_id from mst_city mc,mst_location ml 
                                                where mc.city_id=ml.city_id and ml.dev_id="${newDID}"`,
    { type: db.sequelize.QueryTypes.SELECT });

  let istdateMo = 0

  if (cityData[0] && cityData[0].sun_rise && cityData[0].sun_set) {
    istdateMo = moment(istdate).isBetween(moment(cityData[0].sun_rise, 'HH:mm A'), moment(cityData[0].sun_set, 'HH:mm A'))
  } else {
    istdateMo = moment(istdate).isBetween(moment('06:00:00', 'HH:mm:ss'), moment('18:00:00', 'HH:mm:ss'))
  }
  const DeviceData = await device.findOne({ where: { dev_id: DID }, attributes: ['is_signage'] })
  const cs = decodedData[14].split(":")[1]
  if (DeviceData && DeviceData.is_signage == 1) {
    if (SIGN_status == 1 && !istdateMo && cs > 0) { //Signage green status
      signage_status = 1
    }

    //
    if (SIGN_status == 1 && !istdateMo && cs == 0) { //Signage Red Status 
      signage_status = 3
    }
    if (SIGN_status == 0 && !istdateMo) {
      signage_status = 3
    }

    if (SIGN_status == 0 && istdateMo) {
      signage_status = 2
    }
    if (evt_dt) {
      // checkDeviceSignageStatus({ DID, SIGN_status, evt_dt })
      await signageCheckAndSendAlert({ rawDataID, istdate, DID, value: SIGN_status, istdate: evt_dt, cs })
    }
  }

  // if (AC1_status && caOneValue >= 1) {
  //   ac1_comp = 1
  // } else if (AC1_status && caOneValue < 1) {
  //   ac1_comp = 3
  // }

  // if (AC2_status && caTwoValue >= 1) {
  //   ac2_comp = 1
  // }
  // } else if (AC2_status && caTwoValue < 1) {
  //   ac2_comp = 3
  // }
  // as per V4
  if (AC2_status && TMValue > 30) {
    ac2_comp = 1
  }
  if (AC1_status && TMValue > 30) {
    ac1_comp = 1
  }

  // if (AC1_status && caOneValue >= 1) {
  //   ac1_comp = 1
  // }
  // } else if (AC1_status && caOneValue < 1) {
  //   ac1_comp = 3
  // }

  // if (AC2_status && TMValue <= 30) {
  //   ac2_cooling = 1
  // } else 

  // if (ac2_comp == 1 && TMValue < 30) {
  //   ac2_cooling = 1
  // }
  // if (ac1_comp ==1 && TMValue < 30) {
  //   ac1_cooling = 1
  // }
  //as per V4
  // if (TMValue < 30) {
  //   ac2_cooling = 1
  // }
  // if (TMValue < 30) {
  //   ac1_cooling = 1
  // }
  //as per V5

  if (ac2_comp == 1 && TMValue < 30) {
    ac2_cooling = 1
  }
  if (ac1_comp == 1 && TMValue < 30) {
    ac1_cooling = 1
  }

  await tempCheckAndSendAlert({ rawDataID, istdate, TMValue, DID });
  //TODO : this jugaad for now
  if (TMValue < 18 || TMValue > 60)
    TMValue = 24;



  //          const avgTmp = await db.sequelize.query(`SELECT AVG(TM) as TMAvg FROM trans_expanded_data 
  // WHERE DID= :deviceId AND DATE_FORMAT(evt_dt, '%y-%m-%d %h') = DATE_FORMAT(:istdate, '%y-%m-%d %h')`, {
  // const avgTmp = await db.sequelize.query(`SELECT if(AVG(TM) < 18, 24, AVG(TM)  )as TMAvg
  //   FROM trans_expanded_data WHERE DID= :deviceId
  //   AND evt_dt >= date_sub(now(), interval 24 hour)`, {
  //   type: db.sequelize.QueryTypes.SELECT,
  //   replacements: { istdate: istdate, deviceId: DID }
  // }) // tm value avg
  // console.log(avgTmp)

  let tempAvgData = 0;
  let deviceIdExists = false;
  if (tempDevID.includes(DID)) {
    // for (j = 0; j < global.deviceAvgData.length; j++) {
    //   if (global.deviceAvgData[j].device_id == DID) {
    //     tempAvgData = global.deviceAvgData[j].avg
    //     break;
    //   }
    // }

    // a = "DID: IDFC42383A, IM: EADB84E57F58, HA1: 250.61, HA2: 222.16, HS: 141.10, HL: 0.02, HD: 0.02, HUO: 0.00, HUC: 0.00, 1H:29654, 2H: 29147, CA1: 3.66,CA2: 0.00, CL: 0.00, CS: 0.00, CD: 0.00, CUO: 0.00, CUC: 0.00, TM: 28.70, HM: 31.80, PR: 0, DR: 0, VN:235, VE: 2, VU: 0, RS: 2, TI: 160349081121, AS: 111111,isDoorOpen: 1,humidity: 70.8,phaseToEarthVolt: 233, phaseToNeutralVolt: 234, batteryVolt: 40.4, isOccupied: 1,currTotConsum: 0.149,currACconsum: 0.001, currUPSConsump: 0.193, currDVRConsump: 0.053, totalConsumption: 0, ups: 1583, upsPhaseToEarthVolt: 231,upsPhaseToNeutralVolt: 231"



    let avgData = global.deviceAvgData.find(v => v.device_id == DID)
    tempAvgData = avgData.avg
    tempAvgData = (tempAvgData + TMValue) / 2
    avgData.avg = tempAvgData


    deviceIdExists = true;
  }
  if (!deviceIdExists) {
    const avgData = await db.sequelize.query(`SELECT if(AVG(TM) < 18, 24, AVG(TM)  )as TMAvg
            FROM trans_expanded_data WHERE DID= :deviceId
            AND evt_dt >= date_sub(now(), interval 24 hour)`, {
      type: db.sequelize.QueryTypes.SELECT,
      replacements: { istdate: istdate, deviceId: DID }
    })
    global.deviceAvgData.push({ device_id: DID, avg: avgData[0].TMAvg })
    tempDevID.push(DID)
    tempAvgData = avgData[0].TMAvg
  }


  // if (LOBBY_status) { }
  expandedDataObj.push({
    raw_data_id: rawDataID,
    phase_voltage_diff: 0,
    phase_voltage_status: 0,
    battery_status: 0,
    // dvr_status: dvrStatus,
    ups_voltage_diff: 0,
    // total_consumption: 0,
    // ups: decodedData[38].split(":")[1],
    ups_phase_to_earth_voltage: 0,
    ups_voltage_diff: 0,
    ups_voltage_status: 0,

    DID: decodedData[0] ? decodedData[0].split(":")[1] : null,
    IM: decodedData[1] ? decodedData[1].split(":")[1] : null,
    HAone: decodedData[2] ? checkNumber(decodedData[2].split(":")[1]) : null,
    HAtwo: decodedData[3] ? checkNumber(decodedData[3].split(":")[1]) : null,
    HS: decodedData[4] ? checkNumber(decodedData[4].split(":")[1]) : null,
    HL: decodedData[5] ? checkNumber(decodedData[5].split(":")[1]) : null,
    HD: decodedData[6] ? checkNumber(decodedData[6].split(":")[1]) : null,
    HUO: decodedData[7] ? checkNumber(decodedData[7].split(":")[1]) : null,
    HUC: decodedData[8] ? checkNumber(decodedData[8].split(":")[1]) : null,
    oneH: decodedData[9] ? checkNumber(decodedData[9].split(":")[1]) : null,
    twoH: decodedData[10] ? checkNumber(decodedData[10].split(":")[1]) : null,
    CAone: decodedData[11] ? checkNumber(decodedData[11].split(":")[1]) : null,
    CAtwo: decodedData[12] ? checkNumber(decodedData[12].split(":")[1]) : null,
    CL: decodedData[13] ? checkNumber(decodedData[13].split(":")[1]) : null,
    CS: decodedData[14] ? checkNumber(decodedData[14].split(":")[1]) : null,
    CD: decodedData[15] ? checkNumber(decodedData[15].split(":")[1]) : null,
    CUO: decodedData[16] ? checkNumber(decodedData[16].split(":")[1]) : null,
    CUC: decodedData[17] ? checkNumber(decodedData[17].split(":")[1]) : null,
    IAQ,

    //            TM: decodedData[18] ? decodedData[18].split(":")[1] : null,
    TM: TMValue,
    HM: decodedData[19] ? checkNumber(decodedData[19].split(":")[1]) : null,
    PR: decodedData[20] ? checkNumber(decodedData[20].split(":")[1]) : null,
    DR: decodedData[21] ? checkNumber(decodedData[21].split(":")[1]) : null,
    VN: decodedData[22] ? checkNumber(decodedData[22].split(":")[1]) : null,
    VE: decodedData[23] ? checkNumber(decodedData[23].split(":")[1]) : null,
    VU: decodedData[24] ? checkNumber(decodedData[24].split(":")[1]) : null,
    RS: decodedData[25] ? decodedData[25].split(":")[1] : null,
    TI: decodedData[26] ? decodedData[26].split(":")[1] : null,
    evt_dt: istdate,
    ac1_status,
    ac2_status,
    signage_status,
    lobby_status, // Extra field
    MODE_status: 0,  // Extra field
    MODEM_status: 0, // Extra field
    ac2_conn,
    ac1_conn,
    ac2_comp,
    ac1_comp,
    ac2_cooling,
    ac1_cooling,
    ac2_mode,
    ac1_mode,
    avg_tmp: tempAvgData,
    AS: decodedData[27] ? decodedData[27].split(":")[1] : null,
    is_active: Constants.active, created_by: created_by
  })

}
allcron.processRawData = new cron("0 */2 * * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'processRawData', run_cron_flag: 1, cronStatus: true } })
  // let data =1;
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "processRawData", status: "started", parent_table: "processRawData" });
    let created_by = "RawData_" + cronEntry.id;
    let tt = "";
    let RawDtaID;
    //    let avg_tmp_dev = [];

    try {
      let rawData = await rawdata.findAll({
        limit: 1000, where: {
          is_active: Constants.active,
          is_processed: Constants.active,
        }
      });
      if (rawData.length) {


        for (let i = 0; i < rawData.length; i++) {
          try {
            RawDtaID = rawData[i].raw_data_id
            console.log("I-->" + i);
            let start = Date.now();
            let decodedData = rawData[i].data.replace(/\s/g, '')
            decodedData = decodedData.replace(/[^a-z0-9.:,A-Z ]/g, "")
            decodedData = decodedData.split(",");

            let obj = {};
            decodedData.forEach(string => {
              if (string == "") return;
              const [key, value] = string.split(":");
              obj[key] = value;
            });
            if (Object.values(obj).indexOf(undefined) != -1) {
              await rawdata.update({ is_processed: Constants.reject, updated_by: created_by }, { where: { raw_data_id: RawDtaID } });
              continue;
            }
            if (decodedData[0] && decodedData[0].split(":")[0] == "SW") {
              if (decodedData.length < 30) {
                await rawdata.update({ is_processed: Constants.reject, updated_by: created_by }, { where: { raw_data_id: RawDtaID } });
                continue;
              }

              await euronetData(decodedData, rawData[i].raw_data_id, created_by)
            } else {
              if (decodedData.length < 26) {
                await rawdata.update({ is_processed: Constants.reject, updated_by: created_by }, { where: { raw_data_id: RawDtaID } });
                continue;
              }
              await icontrolData(decodedData, rawData[i].raw_data_id, created_by)
            }

            processedData.push(rawData[i].raw_data_id)
          } catch (err) {
            await rawdata.update({ is_processed: Constants.reject, updated_by: created_by }, { where: { raw_data_id: RawDtaID } });
            cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack || err, created_by)
          }
          // console.log("time 3 " + (Date.now() - start))
        }
        await expandeddata.bulkCreate(expandedDataObj);
        await expanded_data_latest.bulkCreate(expandedDataObj, {
          updateOnDuplicate: ["exp_data_id", "raw_data_id", "IM", "HAone", "HAtwo", "HS", "HL", "HD", "HUO", "HUC", "oneH", "twoH",
            "CAone", "CAtwo", "CL", "CS", "CD", "CUO", "CUC", "TM", "HM", "UE", "PR", "DR", "VN", "VE", "VU", "VB", "RS", "TI", "evt_dt", "trans_expanded_data.AS",
            "is_active", "created_by", "updated_by", "createdAt", "updatedAt", "alert_status", "ac1_status", "ac2_status", "signage_status",
            "ac2_conn", "ac1_conn", "ac2_comp", "ac1_comp", "ac2_cooling", "ac1_cooling", "ac2_mode", "ac1_mode", "avg_tmp", "signage_mode",
            "ac_alert_status", "ups_alert_status", "ac_hrly_alert_status", "temp_alert_status", "sign_alert_status", "is_hrly_processed", "phase_voltage_diff",
            "phase_voltage_status", "battery_status", "ups_phase_to_earth_voltage", "ups_voltage_diff", "ups_voltage_status", "charging", "discharging", "IAQ",
            "device_mode", "modem_status", "dvr_status", "lobby_status"]
        });
        console.log('====================================');
        console.log("sadsadgagdagcdga");
        console.log('====================================');
        await rawdata.update({ is_processed: Constants.isProcessed, updated_by: created_by }, { where: { raw_data_id: { [Op.in]: processedData }, is_processed: Constants.active } });
        expandedDataObj = [];
        processedData = [];
        tempDevID = [];
      }
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      await rawdata.update({ is_processed: Constants.reject, updated_by: created_by }, { where: { raw_data_id: RawDtaID } });
      await systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
      await cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    }
    await systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  } else {
    winston.info("Cron processRawData error-----> Cron flag set to 0 or cronStatus is false");
  }

});

allcron.airQualityData = new cron("0 0 0 1 * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'airQualityData', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "airQualityData", status: "started", parent_table: "mst_location" });
    let created_by = "airQualityData_" + cronEntry.id;

    try {
      const cityDta = await db.sequelize.query(
        `SELECT distinct ml.city_id,city_name,state_name FROM mst_city mc,mst_state ms, mst_location ml where mc.city_id=ml.city_id and ms.state_id=ml.state_id`,
        { type: db.sequelize.QueryTypes.SELECT });

      if (cityDta.length) {
        for (let i = 0; i < cityDta.length; i++) {
          const respData = await (await fetch(`https://api.airvisual.com/v2/city?city=${cityDta[i].city_name}&state=${cityDta[i].state_name}&country=India&key=2a04845e-8314-48e6-af7c-5a74a01b468e`)).json()
          if (respData.status == "success")
            await location.update({ IAQ: respData.data.current.pollution.aqius }, { where: { city_id: cityDta[i].city_id } })

        }
      }

      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    }
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  } else {
    winston.info("Cron airQualityData error-----> Cron flag set to 0 or cronStatus is false");
  }

});

allcron.processExpandedDataHourly = new cron("0 */15 * * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'processExpandedDataHr', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "processExpandedDataHr", status: "started", parent_table: "trans_hourly_data" });
    let created_by = "hourlydata_" + cronEntry.id;

    try {
      let expandeddataList = await db.sequelize.query(`
          SELECT
          DID,
            date_format(evt_dt, '%y-%m-%d %H') evt_date_hr,
            cast(date_format(evt_dt, '%H') as unsigned) evt_hour,
            min(evt_dt) evt_dt,
            min(HAone) HAone_min, max(HAone) HAone_max,
            min(HAtwo) HAtwo_min, max(HAtwo) HAtwo_max,
            avg(TM) TM_avg,
            sum(HUC) sum_ups,
            sum(HD) sum_dvr,
            min(HS) HS_min, max(HS) HS_max,
            min(HL) HL_min, max(HL) HL_max,
            min(HD) HD_min, max(HD) HD_max,
            min(HUO) HUO_min, max(HUO) HUO_max,
            min(HUC) HUC_min, max(HUC) HUC_max,
            min(oneH) oneH_min, max(oneH) oneH_max,
            min(twoH) twoH_min, max(twoH) twoH_max,
            min(oneH) oneH_min, max(oneH) oneH_max,
            min(twoH) twoH_min, max(twoH) twoH_max
          FROM trans_expanded_data ted
          WHERE  ted.evt_dt > date_format(NOW()+ INTERVAL 270 MINUTE , '%Y-%m-%d %H:00:00')   and ( HAone !=0 or HAtwo!=0) and YEAR(now())=YEAR(evt_dt) 
         --		and ted.evt_dt > date_add( NOW(),   INTERVAL 180 minute)
         -- WHERE ted.evt_dt > SUBTIME(NOW(), '06:00:00')
          -- WHERE ted.evt_dt > DATE_SUB(NOW(), interval 1 day) and(HAone != 0 or HAtwo != 0)
          GROUP BY DID, date_format(evt_dt, '%y-%m-%d %H'),
            cast(date_format(evt_dt, '%H') as unsigned)
          ORDER BY DID desc 
            `, { type: db.sequelize.QueryTypes.SELECT });

      if (expandeddataList.length) {
        var tmp_did = "", tmp_max = "";

        for (let i = 0; i < expandeddataList.length; i++) {
          const element = expandeddataList[i];

          // if(temp_did != element.DID){
          //   temp_did=element.DID;
          //   // temp_max =
          // }
          // flag ? fs.appendFileSync('deviceTrack.txt', "List \n" + JSON.stringify(element)) : null

          // const hAOneMin = element.HAone_min > 10 ?
          //   currency(element.HAone_min, { precision: Constants.float_point }).divide(1000).value : element.HAone_min;

          // const hAOneMax = element.HAone_max > 10 ?
          //   currency(element.HAone_max, { precision: Constants.float_point }).divide(1000).value : element.HAone_max;

          // const hATwoMin = element.HAtwo_min > 10 ?
          //   currency(element.HAtwo_min, { precision: Constants.float_point }).divide(1000).value : element.HAtwo_min;

          // const hATwoMax = element.HAtwo_max > 10 ?
          //   currency(element.HAtwo_max, { precision: Constants.float_point }).divide(1000).value : element.HAtwo_max;

          // const hAOneMin = currency(element.HAone_min, { precision: Constants.float_point }).divide(1000).value;

          // const hAOneMax = currency(element.HAone_max, { precision: Constants.float_point }).divide(1000).value;

          // const hATwoMin = currency(element.HAtwo_min, { precision: Constants.float_point }).divide(1000).value;

          // const hATwoMax = currency(element.HAtwo_max, { precision: Constants.float_point }).divide(1000).value;
          //Divide by 1000 has been removed

          // if(tmp_did != element.DID){
          //   tmp_did = element.DID;
          //   tmp_max = currency(element.HAone_min, { precision: Constants.float_point }).value;
          // }



          let hAOneMin = currency(element.HAone_min, { precision: Constants.float_point }).value;
          // let hAOneMin = tmp_max;

          let hAOneMax = currency(element.HAone_max, { precision: Constants.float_point }).value;

          // tmp_max = hAOneMax;

          let hATwoMin = currency(element.HAtwo_min, { precision: Constants.float_point }).value;

          let hATwoMax = currency(element.HAtwo_max, { precision: Constants.float_point }).value;



          let HS_cons = 0
          if ((element.HS_max - element.HS_min) > 0.001) {
            HS_cons = currency(element.HS_max, { precision: Constants.float_point }).subtract(element.HS_min).value
          }

          const newHourlyData = {
            DID: element.DID,
            evt_dt: element.evt_dt,
            evt_date_hr: element.evt_date_hr,
            ups_sum: element.sum_ups,
            dvr_sum: element.sum_dvr,
            HAone_min: element.HAone_min,
            HAone_max: element.HAone_max,
            HAone_cons: currency(hAOneMax, { precision: Constants.float_point }).subtract(hAOneMin).value,
            HAtwo_min: element.HAtwo_min,
            HAtwo_max: element.HAtwo_max,
            HAtwo_cons: currency(hATwoMax, { precision: Constants.float_point }).subtract(hATwoMin).value,
            HS_min: element.HS_min,
            HS_max: element.HS_max,
            HS_cons: HS_cons,
            HL_min: element.HL_min,
            HL_max: element.HL_max,
            HL_cons: currency(element.HL_max, { precision: Constants.float_point }).subtract(element.HL_min).value,
            HD_min: element.HD_min,
            HD_max: element.HD_max,
            HD_cons: currency(element.HD_max, { precision: Constants.float_point }).subtract(element.HD_min).value,
            HUO_min: element.HUO_min,
            HUO_max: element.HUO_max,
            HUO_cons: currency(element.HUO_max, { precision: Constants.float_point }).subtract(element.HUO_min).value,
            HUC_min: element.HUC_min,
            HUC_max: element.HUC_max,
            HUC_cons: currency(element.HUC_max, { precision: Constants.float_point }).subtract(element.HUC_min).value,
            oneH_min: element.oneH_min,
            oneH_max: element.oneH_max,
            oneH_cons: currency(element.oneH_max, { precision: Constants.float_point }).subtract(element.oneH_min).value,
            twoH_min: element.twoH_min,
            twoH_max: element.twoH_max,
            twoH_cons: currency(element.twoH_max, { precision: Constants.float_point }).subtract(element.twoH_min).value,
            TM_avg: element.TM_avg,
            evt_hr: element.evt_hour,
            is_active: 1,
            created_by: created_by,
            updated_by: created_by,
          };

          const result = await hourlydata.findOrCreate({
            where: { DID: newHourlyData.DID, evt_date_hr: newHourlyData.evt_date_hr, },
            defaults: newHourlyData
          });
          // flag ? fs.appendFileSync('deviceTrack.txt', "\nCreate Data \n" + JSON.stringify(result[0])) : null
          if (!result[1]) {
            result[0].evt_dt = newHourlyData.evt_dt;
            result[0].evt_date_hr = newHourlyData.evt_date_hr;
            result[0].dvr_sum = newHourlyData.dvr_sum;
            result[0].ups_sum = newHourlyData.ups_sum;
            result[0].HAone_min = newHourlyData.HAone_min;
            result[0].HAone_max = newHourlyData.HAone_max;
            result[0].HAone_cons = newHourlyData.HAone_cons;
            result[0].HAtwo_min = newHourlyData.HAtwo_min;
            result[0].HAtwo_max = newHourlyData.HAtwo_max;
            result[0].HAtwo_cons = newHourlyData.HAtwo_cons;
            result[0].HS_min = newHourlyData.HS_min;
            result[0].HS_max = newHourlyData.HS_max;
            result[0].HS_cons = newHourlyData.HS_cons;
            result[0].HL_min = newHourlyData.HL_min;
            result[0].HL_max = newHourlyData.HL_max;
            result[0].HL_cons = newHourlyData.HL_cons;
            result[0].HD_min = newHourlyData.HD_min;
            result[0].HD_max = newHourlyData.HD_max;
            result[0].HD_cons = newHourlyData.HD_cons;
            result[0].HUO_min = newHourlyData.HUO_min;
            result[0].HUO_max = newHourlyData.HUO_max;
            result[0].HUO_cons = newHourlyData.HUO_cons;
            result[0].HUC_min = newHourlyData.HUC_min;
            result[0].HUC_max = newHourlyData.HUC_max;
            result[0].HUC_cons = newHourlyData.HUC_cons;
            result[0].oneH_min = newHourlyData.oneH_min;
            result[0].oneH_max = newHourlyData.oneH_max;
            result[0].oneH_cons = newHourlyData.oneH_cons;
            result[0].twoH_min = newHourlyData.twoH_min;
            result[0].twoH_max = newHourlyData.twoH_max;
            result[0].twoH_cons = newHourlyData.twoH_cons;
            result[0].TM_avg = newHourlyData.TM_avg;
            result[0].evt_hr = newHourlyData.evt_hr;
            result[0].updated_by = newHourlyData.updated_by;
            await result[0].save();
            console.log(`hourlyRepo duplicate ${newHourlyData.DID} ${newHourlyData.evt_date_hr} `)
          }


          // acCheckAndSendAlert({
          //   DID: newHourlyData.DID, Oprator: 'AND',
          //   HAone: newHourlyData.HAone_cons,
          //   HAtwo: newHourlyData.HAtwo_cons,
          // })
        }
      }
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      winston.info(`Cron processExpandedDataHourly error-----> ${err.stack} `);
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    }
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  } else {
    winston.info("Cron processExpandedDataHourly error----->", "Cron flag set to 0 or cronStatus is false");
  }

});

allcron.prsExpandDataBy15Minutes = new cron("0 */15 * * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'prsExpandDataBy15Minutes', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "prsExpandDataBy15Minutes", status: "started", parent_table: "trans_expanded_data" });
    let created_by = "15Minutesdata_" + cronEntry.id;

    try {
      let expandeddataList = await db.sequelize.query(`
      SELECT
         DID,
         max(VN) VN_max,
         max(VU) VU_max,
         max(phase_voltage_diff)rawEarth_max ,
         max(ups_voltage_diff) upsEarth_max,
         max(TM) TM_max,
         min(evt_dt) min_evt_dt
      FROM trans_expanded_data ted
      WHERE 
        ted.evt_dt > date_add( NOW(),   INTERVAL 270 minute)
       and ted.evt_dt < date_add( NOW(),   INTERVAL 1000 minute)
          and ( HAone !=0 or HAtwo!=0)  
      GROUP BY DID, 
          timestampdiff(second, date_format(ted.evt_dt , '%Y-%m-%d'), ted.evt_dt) div 900 
 
         ORDER BY DID , min_evt_dt desc ;
            `, { type: db.sequelize.QueryTypes.SELECT });

      if (expandeddataList.length) {
        //        dataLog.info("-----------Started---------- length " + expandeddataList.length)

        for (let i = 0; i < expandeddataList.length; i++) {
          const element = expandeddataList[i];
          //          dataLog.info(`select query Data for expandeddataList[${i}]"-->
          //           ${JSON.stringify(expandeddataList[i])}`)

          const new15MinutesData = {
            DID: element.DID,
            evt_dt: element.evt_dt,
            VN_max: element.VN_max,
            VU_max: element.VU_max,
            rawEarth_max: element.rawEarth_max,
            upsEarth_max: element.upsEarth_max,
            TM_max: element.TM_max,
            min_evt_dt: element.min_evt_dt,
            //       is_active: 1,
            createdby: created_by,
            updatedby: created_by,
          };

          const result = await upsChart.findOrCreate({
            where: { DID: new15MinutesData.DID, min_evt_dt: new15MinutesData.min_evt_dt, },
            defaults: new15MinutesData
          });
          //   dataLog.info(` where clause DID : ${new15MinutesData.DID} # min_evt_dt: ${new15MinutesData.min_evt_dt}`)
          //   dataLog.info(`findOrCreate query Data for result[1] -->${JSON.stringify(result[1])}`)

          // flag ? fs.appendFileSync('deviceTrack.txt', "\nCreate Data \n" + JSON.stringify(result[0])) : null
          if (!result[1]) {
            result[0].evt_dt = new15MinutesData.evt_dt;
            result[0].DID = new15MinutesData.DID;
            result[0].VN_max = new15MinutesData.VN_max;
            result[0].VU_max = new15MinutesData.VU_max;
            result[0].rawEarth_max = new15MinutesData.rawEarth_max;
            result[0].upsEarth_max = new15MinutesData.upsEarth_max;
            result[0].TM_max = new15MinutesData.TM_max;
            result[0].min_evt_dt = new15MinutesData.min_evt_dt;
            result[0].updatedby = new15MinutesData.updatedby;

            await result[0].save();

            //            dataLog.info(`findOrCreate query Data save for result[0] -->,${JSON.stringify(result[0])}`)

            //            console.log(`duplicate ${new15MinutesData.DID} ${new15MinutesData.max_evt_dt} `)
          }
          //Not in use
          // acCheckAndSendAlert({
          //   DID: newHourlyData.DID, Oprator: 'AND',
          //   HAone: newHourlyData.HAone_cons,
          //   HAtwo: newHourlyData.HAtwo_cons,
          // })

        }
        dataLog.info("---------------End----------------")

      }
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      winston.info(`Cron prsExpandDataBy15Minutes error-----> ${err.stack} `);
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    }
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  } else {
    winston.info("Cron prsExpandDataBy15Minutes error----->", "Cron flag set to 0 or cronStatus is false");
  }

});

let tokenStore = null
const updateDeviceApiTokenFunc = async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'updateDeviceApiToken', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "updateDeviceApiToken", status: "started", parent_table: "trans_hourly_data" });
    let created_by = "updateDeviceApiToken_" + cronEntry.id;

    try {
      const [err, data] = await request.post({
        url: `${Constants.cloudBuildApi.baseUrl}${Constants.cloudBuildApi.endPoints.login.uri}`,
        body: Constants.cloudBuildApi.endPoints.login.body,
      })
      if (err) cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
      else {
        tokenStore = data.data.token;
        cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
      }
    } catch (err) {
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    }
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  } else {
    winston.info("Cron updateDeviceApiToken error----->", "Cron flag set to 0 or cronStatus is false");
  }
}


// allcron.updateDeviceApiToken = new cron("0 */23 * * * *", updateDeviceApiTokenFunc);

// allcron.updateDeviceStatus = new cron("0 */1 * * * *", async function () {
allcron.updateDeviceStatus = new cron("0 */10 * * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'updateDeviceStatus', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "updateDeviceStatus", status: "started", parent_table: "trans_hourly_data" });
    let created_by = "updateDeviceStatus_" + cronEntry.id;
    if (!tokenStore) await updateDeviceApiTokenFunc();
    if (tokenStore) {
      try {
        let deviceCount = (await db.sequelize.query(`
          SELECT COUNT(*) as total FROM mst_location;
      `, { type: db.sequelize.QueryTypes.SELECT }))[0].total;

        if (deviceCount) {
          const threshold = 50;

          const getDeviceStatus = ({ id }) => request.get({
            url: `${Constants.cloudBuildApi.baseUrl}${Constants.cloudBuildApi.endPoints.devicestate.uri}`,
            headers: { ...Constants.cloudBuildApi.endPoints.devicestate.headers, Authorization: tokenStore },
            query: { deviceid: id }
          })
          for (let skip = 0; skip < deviceCount; skip += threshold) {

            let deviceList = await db.sequelize.query(
              `SELECT loc_id,name,hk_dev_id FROM mst_location LIMIT ${skip}, ${threshold}`,
              { type: db.sequelize.QueryTypes.SELECT }
            );

            const bulkAuditData = [];
            for (let index = 0; index < deviceList.length; index++) {
              const dev = deviceList[index];

              // await Promise.allSettled(deviceList.map(async dev => {
              const [err, data] = await getDeviceStatus({ id: dev.hk_dev_id })
              try {
                if (!err && data) {
                  const otherkey = data.data.state === 'OFFLINE' ? '' : `,dev_status_last_recived='${moment().add(330, 'minutes').format('YYYY-MM-DD HH:mm:ss')}'`
                  await db.sequelize.query(
                    `UPDATE mst_location 
                     SET live_status=${data.data.state === 'OFFLINE' ? 0 : 1}${otherkey}
                     WHERE hk_dev_id='${dev.hk_dev_id}'
                    `, { type: db.sequelize.QueryTypes.UPDATE }
                  );
                  if (data.data.state === 'OFFLINE') {
                    console.log("Dev id : " + dev.hk_dev_id + " is offline")
                  }
                  let errorFlag = false;
                  const recordStatus = data.data.state === 'OFFLINE' ? 1 : 0
                  const alertType = Constants.alertTypeId.DeviceStatus
                  const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
                    where: { DID: dev.hk_dev_id, alert_type: alertType },
                    defaults: {
                      DID: dev.hk_dev_id, alert_type: alertType,
                      is_active: recordStatus, created_by
                    }
                  });
                  if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
                    transAlertStatusDoc.is_active = 0;
                    transAlertStatusDoc.updated_by = created_by;
                    await transAlertStatusDoc.save();
                  } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
                    transAlertStatusDoc.is_active = 1;
                    transAlertStatusDoc.updated_by = created_by;
                    await transAlertStatusDoc.save();
                    errorFlag = true;
                  }
                  if (errorFlag || (docStatus && recordStatus)) {


                    //   if (data.data.state == 'OFFLINE') {

                    creatAlertNotificationToOrg({
                      loc_id: dev.loc_id,
                      DID: dev.hk_dev_id,
                      loc_name: dev.name,
                      message: `Site is Down `,
                      alertType: Constants.alertStatus.DeviceStatus,
                      columnValue: 'OFFLINE',
                      rxtype: ['internal'],
                      severity: 'Minor', priority: 'Low',
                    })
                  }

                  // await db.sequelize.query(
                  //   `INSERT INTO adt_dev_status(dev_id,evt_dt,data) 
                  //    VALUES('${dev.hk_dev_id}','${new Date()},'${JSON.stringify(data)}')
                  //   `, { type: db.sequelize.QueryTypes.INSERT }
                  // );
                }
                bulkAuditData.push({
                  dev_id: dev.hk_dev_id,
                  evt_dt: new Date(),
                  data: JSON.stringify({ data, err }),
                  status: 1,
                })
              } catch (error) {
                console.log(error)
              }
            }
            // }))
            await auditDevStatus.bulkCreate(bulkAuditData)
            if (deviceList && deviceList.length < threshold) {
              break;
            }
          }

        }
        cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
      } catch (err) {
        cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
      }
      systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
    } else {
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, 'Token store is empty', created_by)
      systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
    }
  } else {
    winston.info("Cron updateDeviceStatus error----->", "Cron flag set to 0 or cronStatus is false");
  }

});

allcron.updateDeviceLastStatus = new cron("0 */1 * * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'updateDeviceLastStatus', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "updateDeviceLastStatus", status: "started", parent_table: "trans_hourly_data" });
    let created_by = "updateDeviceLastStatus_" + cronEntry.id;
    try {

      let deviceList = await db.sequelize.query(`SELECT * FROM mst_location`, { type: db.sequelize.QueryTypes.SELECT });

      for (let i = 0; i < deviceList.length; i++) {
        const selectedDevice = deviceList[i];
        const now = moment(new Date()); //todays date
        const end = moment(selectedDevice.dev_status_last_recived); // another date
        const duration = moment.duration(now.diff(end));
        selectedDevice.duration = duration.asMinutes();
        message = ""
        let dev_live_status = 0;
        if (selectedDevice.duration <= 10 && selectedDevice.dev_live_status != 1) {
          dev_live_status = Constants.active
          const deviceupdate = await db.sequelize.query(
            `UPDATE mst_location SET dev_live_status=${dev_live_status}
             WHERE loc_id =${selectedDevice.loc_id}`,
            { type: db.sequelize.QueryTypes.UPDATE }
          )
          // if (deviceupdate[1] > 0) message = `Device ${selectedDevice.dev_id} is running`;
        } else if (selectedDevice.duration > 20 && selectedDevice.duration <= 30 && selectedDevice.dev_live_status != 2) {
          dev_live_status = 2
          const deviceupdate = await db.sequelize.query(
            `UPDATE mst_location SET dev_live_status=${dev_live_status}
              WHERE loc_id =${selectedDevice.loc_id}`,
            { type: db.sequelize.QueryTypes.UPDATE }
          )
          if (deviceupdate[1] > 0) message = `Device ${selectedDevice.dev_id} data is not coming from 5 minute`;
        } else if (selectedDevice.duration > 30 && selectedDevice.dev_live_status != 3) {
          dev_live_status = 3
          const deviceupdate = await db.sequelize.query(
            `UPDATE mst_location SET dev_live_status=${dev_live_status}
             WHERE loc_id =${selectedDevice.loc_id}`,
            { type: db.sequelize.QueryTypes.UPDATE }
          )
          if (deviceupdate[1] > 0 && !selectedDevice.last_data_received) {
            message = `Device ${selectedDevice.dev_id} data is not alive`;
          } else if (deviceupdate[1] > 0) {
            message = `Device ${selectedDevice.dev_id} data is not coming from 30 minute`;
          }
        }

        if (message) {
          let errorFlag = false;
          const recordStatus = dev_live_status > 1 ? 1 : 0;
          const alertType = Constants.alertTypeId.DeviceLastStatus
          const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
            where: { DID: selectedDevice.dev_id, alert_type: alertType },
            defaults: {
              DID: selectedDevice.dev_id, alert_type: alertType,
              is_active: recordStatus, created_by
            }
          });

          if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
            transAlertStatusDoc.is_active = 0;
            transAlertStatusDoc.updated_by = created_by;
            await transAlertStatusDoc.save();
          } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
            transAlertStatusDoc.is_active = 1;
            transAlertStatusDoc.updated_by = created_by;
            await transAlertStatusDoc.save();
            errorFlag = true;
          }

          if (errorFlag || (docStatus && recordStatus)) {
            creatAlertNotificationToOrg({
              loc_id: selectedDevice.loc_id,
              DID: selectedDevice.dev_id,
              loc_name: selectedDevice.name,
              message: message,
              alertType: Constants.alertStatus.DeviceStatus,
              columnValue: dev_live_status,
              rxtype: ['internal'],
              severity: 'High', priority: 'High',
            })
          }

        }

      }
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    } finally {
      systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
    }
  } else {
    winston.info("Cron updateDeviceLastStatus error----->", "Cron flag set to 0 or cronStatus is false");
  }
});

allcron.lastActiveData = new cron("0 */1 * * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'lastActiveData', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "lastActiveData", status: "started", parent_table: "mst_location" });
    let created_by = "lastActiveData_" + cronEntry.id;
    try {

      const deviceList = await db.sequelize.query(`SELECT dev_id,dev_status_last_recived FROM mst_location where is_active=1`, { type: db.sequelize.QueryTypes.SELECT });
      let message = "";
      for (let i = 0; i < deviceList.length; i++) {
        // let device = deviceList[i].dev_id.slice(0, deviceList[i].dev_id.length) + "A"
        // let device = deviceList[i].dev_id.split('').splice(0, deviceList[i].dev_id.length - 1).join('')
        // const lastData = await db.sequelize.query(`SELECT evt_dt FROM trans_expanded_data_latest where DID = "${device}" order by evt_dt desc limit 1 `, { type: db.sequelize.QueryTypes.SELECT })
        if (deviceList[i] && deviceList[i].dev_status_last_recived) {

          const now = moment(new Date()).add(330, "minutes"); //todays date
          const end = moment(deviceList[i].dev_status_last_recived); // another date

          let duration = moment.duration(now.diff(end));
          duration = duration.asMinutes();
          message = ""
          let dev_live_status = 0;
          if (duration > 30) {
            message = `Device ${deviceList[i].dev_id} data is not coming from 30 minutes`;
            // if (deviceupdate[1] > 0) message = `Device ${selectedDevice.dev_id} is running`;
          }
          // message = `Device ${selectedDevice.dev_id} data is not coming from 30 minute`;

          // if (message) {
          let errorFlag = false;
          const recordStatus = message.length > 1 ? 1 : 0;
          const alertType = Constants.alertTypeId.DeviceHealthStatus
          const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
            where: { DID: deviceList[i].dev_id, alert_type: alertType },
            defaults: {
              DID: deviceList[i].dev_id, alert_type: alertType,
              is_active: recordStatus, created_by
            }
          });

          if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
            transAlertStatusDoc.is_active = 0;
            transAlertStatusDoc.updated_by = created_by;
            await transAlertStatusDoc.save();
          } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
            transAlertStatusDoc.is_active = 1;
            transAlertStatusDoc.updated_by = created_by;
            await transAlertStatusDoc.save();
            errorFlag = true;
          }

          if (errorFlag || (docStatus && recordStatus)) {
            await creatAlertNotificationToOrg({
              loc_id: deviceList[i].loc_id,
              DID: deviceList[i].dev_id.slice(0, deviceList[i].dev_id.length) + "A",
              loc_name: deviceList[i].name,
              message: message,
              alertType: Constants.alertStatus.DeviceStatus,
              columnValue: dev_live_status,
              rxtype: ['internal'],
              severity: 'High', priority: 'High',
            })
          }
        }
        // }

      }
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    } finally {
      systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
    }
  } else {
    winston.info("Cron lastActiveData error----->", "Cron flag set to 0 or cronStatus is false");
  }
});

const timeAndValueAlert = async () => {
  let data = await systemcontrol.findOne({ where: { cron_name: 'timeAndValueAlert', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "timeAndValueAlert", status: "started", parent_table: "trans_hourly_data" });
    let created_by = "timeAndValueAlert_" + cronEntry.id;
    try {
      const alertsList = await db.sequelize.query(`SELECT * FROM mst_alert`,
        { type: db.sequelize.QueryTypes.SELECT });
      let expandedDataList = await db.sequelize.query(`
          SELECT * FROM trans_expanded_data ted
          JOIN mst_location ml ON ted.DID like concat(ml.dev_id,'%')
          where alert_status = 0 order by evt_dt Limit 300
        `, { type: db.sequelize.QueryTypes.SELECT });
      const trans_expanded_data_ids = [];
      for (let posi = 0; posi < expandedDataList.length; posi++) {
        const expandedData = expandedDataList[posi];

        if (expandedData.dev_id) {
          for (let index = 0; index < alertsList.length; index++) {
            const alert = alertsList[index];
            const columnValue = expandedData[alert.channel]
            const forDevice = (alert.allowed_dev || '').split(',').filter(ele => ele)

            if (forDevice.length && !forDevice.includes(expandedData.DID)) {
              break;
            }
            let msgType = ''
            // if (alert.alert_type_id == Constants.alertType.missingData) { }

            if (![null, undefined].includes(columnValue)) {
              let error = false;
              if (alert.alert_type_id == Constants.alertType.rangeValue) {
                if (columnValue > alert.max_val || columnValue < alert.min_val) {
                  error = true;
                  msgType = Constants.msgType.value_range;
                }
              }

              if (alert.alert_type_id == Constants.alertType.timeValue) {
                const dayOfWeek = new Date().getDay();
                const dayStatus = alert.allowed_days.split(',')[dayOfWeek];
                if (dayStatus) {
                  const evt_dt = new Date(expandedData.evt_dt);
                  if (evt_dt > new Date(alert.max_time) || evt_dt > new Date(alert.min_time)) {
                    error = true;
                  }
                } else {
                  error = true;
                  msgType = Constants.msgType.time_range;
                }
              }

              if (error) {
                if (!alert.is_mute) {
                  if (alert.comm_sms) {
                    Constants.notificationMem.mobile.forEach((mobNum) => {
                      NotificationsService.create({
                        noti_type: Constants.notificationType.sms,
                        mobile: mobNum,
                        msg: alert.issue_text,
                        user_id: expandedData.loc_id,
                        msg_id: alert.alert_type_id,
                        msg_type: msgType,
                      });
                    })
                  }
                  if (alert.comm_email) {
                    NotificationsService.create({
                      noti_type: Constants.notificationType.email,
                      email: Constants.notificationMem.email,
                      msg: alert.issue_text,
                      user_id: expandedData.loc_id,
                      msg_id: alert.alert_type_id,
                      msg_type: msgType,
                    });
                  }
                }
                var minutesToAdd = 330;
                var currentDate = new Date();
                var futureDate = new Date(currentDate.getTime() + minutesToAdd * 60000).toJSON().slice(0, 19).replace('T', ' ');
                //const nowDate = new Date().toJSON().slice(0, 19).replace('T', ' ');
                const nowDate = futureDate;
                const siteAlrtNew = `INSERT INTO site_alerts (location,issue,alert_id,is_active,raised_on,createdAt,updatedAt,Loc_name,observed_val,severity,priority) VALUES
                     (${expandedData.loc_id},'${alert.issue_text}',${alert.alert_type_id},1,'${nowDate}','${nowDate}','${nowDate}', '${expandedData.name}',${columnValue},${alert.severity},${alert.priority})`
                await db.sequelize.query(siteAlrtNew, { type: db.sequelize.QueryTypes.INSERT })
              }
            }
          }
          trans_expanded_data_ids.push(expandedData.exp_data_id)
        }
      }

      if (trans_expanded_data_ids.length) await db.sequelize.query(
        `UPDATE trans_expanded_data 
         SET alert_status=1, updatedAt = '${new Date().toJSON().slice(0, 19).replace('T', ' ')}'
         where exp_data_id IN (${trans_expanded_data_ids.join(',')})`,
        { type: db.sequelize.QueryTypes.UPDATE })
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      console.log(err)
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    } finally {
      systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
    }
  } else {
    winston.info("Cron timeAndValueAlert error----->", "Cron flag set to 0 or cronStatus is false");
  }
}

allcron.timeAndValueAlert = new cron("0 */1 * * * *", timeAndValueAlert);

allcron.transExpandedDataArchive = new cron("0 */23 * * * *", async () => {
  let data = await systemcontrol.findOne({ where: { cron_name: 'transExpandedDataArchive', run_cron_flag: 1, cronStatus: true } })
  if (!data) return winston.info("Cron transExpandedDataArchive error----->", "Cron flag set to 0 or cronStatus is false");

  await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
  let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "transExpandedDataArchive", status: "started", parent_table: "trans_extanded_data" });
  let created_by = "transExpandedDataArchive_" + cronEntry.id;
  const t = await db.sequelize.transaction();
  try {

    await db.sequelize.query(`
      INSERT INTO arch_expanded_data
      SELECT * FROM trans_expanded_data
      WHERE evt_dt < DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY),'%y-%m-%d 00:00:00')
    `, { type: db.sequelize.QueryTypes.INSERT, transaction: t });

    await db.sequelize.query(`
      DELETE FROM trans_expanded_data
      WHERE evt_dt < DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY),'%y-%m-%d 00:00:00')
    `, { type: db.sequelize.QueryTypes.DELETE, transaction: t });

    cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    await t.commit();
  } catch (err) {
    await t.rollback();
    console.error(err)
    cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
  } finally {
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  }
});

allcron.transRawDataArchive = new cron("0 */23 * * * *", async () => {
  let data = await systemcontrol.findOne({ where: { cron_name: 'transRawDataArchive', run_cron_flag: 1, cronStatus: true } })
  if (!data) return winston.info("Cron transRawDataArchive error----->", "Cron flag set to 0 or cronStatus is false");

  await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
  let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "transRawDataArchive", status: "started", parent_table: "trans_extanded_data" });
  let created_by = "transRawDataArchive_" + cronEntry.id;
  const t = await db.sequelize.transaction();
  try {

    await db.sequelize.query(`
      INSERT INTO arch_raw_data
      SELECT * FROM trans_raw_data
      WHERE createdAt < DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY),'%y-%m-%d 00:00:00')
    `, { type: db.sequelize.QueryTypes.INSERT, transaction: t });

    await db.sequelize.query(`
      DELETE FROM trans_raw_data
      WHERE createdAt < DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY),'%y-%m-%d 00:00:00')
    `, { type: db.sequelize.QueryTypes.DELETE, transaction: t });

    cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    await t.commit();
  } catch (err) {
    await t.rollback();
    console.error(err)
    cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
  } finally {
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  }
});

allcron.transHourlyDataArchive = new cron("0 */23 * * * *", async () => {
  // allcron.transExpandedDataArchive = new cron("*/1 * * * * *", async () => {
  let data = await systemcontrol.findOne({ where: { cron_name: 'transHourlyDataArchive', run_cron_flag: 1, cronStatus: true } })
  if (!data) return winston.info("Cron transHourlyDataArchive error----->", "Cron flag set to 0 or cronStatus is false");

  await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
  let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "transHourlyDataArchive", status: "started", parent_table: "trans_extanded_data" });
  let created_by = "transHourlyDataArchive_" + cronEntry.id;
  const t = await db.sequelize.transaction();
  try {
    await db.sequelize.query(`
      INSERT INTO arch_hourly_data
      SELECT * FROM trans_hourly_data
      WHERE evt_dt < DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY),'%y-%m-%d 00:00:00')
    `, { type: db.sequelize.QueryTypes.INSERT, transaction: t });

    await db.sequelize.query(`
      DELETE FROM trans_hourly_data
      WHERE evt_dt < DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 5 DAY),'%y-%m-%d 00:00:00')
    `, { type: db.sequelize.QueryTypes.DELETE, transaction: t });

    cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    await t.commit();
  } catch (err) {
    await t.rollback();
    console.error(err)
    cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
  } finally {
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  }
});

allcron.ACAlert = new cron("0 */1 * * * *", async () => {
  let data = await systemcontrol.findOne({ where: { cron_name: 'ACAlert', run_cron_flag: 1, cronStatus: true } })
  if (!data) return winston.info("Cron ACAlert error----->", "Cron flag set to 0 or cronStatus is false");

  await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
  let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "ACAlert", status: "started", parent_table: "trans_extanded_data" });
  let created_by = "ACAlert_" + cronEntry.id;
  try {
    const alertType = Constants.alertStatus.AC
    const expandedDataList = await db.sequelize.query(`
          SELECT * FROM trans_expanded_data ted
          JOIN mst_location ml ON ted.DID like concat(ml.dev_id,'%')
          where ac_alert_status = 0 order by evt_dt Limit 100
        `, { type: db.sequelize.QueryTypes.SELECT });

    const trans_expanded_data_ids = [];
    const thresholdValue = 0;
    for (let posi = 0; posi < expandedDataList.length; posi++) {
      const expdData = expandedDataList[posi];
      try {
        const columns = [];
        let errorFlag = false;

        if (expdData.HAone <= thresholdValue) {
          columns.push('HAone')
        }
        if (expdData.HAtwo <= thresholdValue) {
          columns.push('HAtwo')
        }
        const recordStatus = columns.length == 2 ? 1 : 0
        const [transAlertStatusDoc, docStatus] = await transAlertStatus.findOrCreate({
          where: { DID: expdData.DID, alert_type: alertType },
          defaults: {
            DID: expdData.DID, alert_type: alertType,
            is_active: recordStatus, created_by
          }
        });

        if (!docStatus && transAlertStatusDoc.dataValues.is_active && !recordStatus) {
          transAlertStatusDoc.is_active = 0;
          transAlertStatusDoc.updated_by = created_by;
          await transAlertStatusDoc.save();
        } else if (!docStatus && !transAlertStatusDoc.dataValues.is_active && recordStatus) {
          transAlertStatusDoc.is_active = 1;
          transAlertStatusDoc.updated_by = created_by;
          await transAlertStatusDoc.save();
          errorFlag = true;
        }

        if (errorFlag || (docStatus && recordStatus)) {
          if (!expdData.is_mute) {
            const message = `Device ${expdData.DID} ${columns.join(',')} values gone less then ${thresholdValue}`;

            if (expdData.comm_sms) {
              Constants.notificationMem.mobile.forEach((mobNum) => {
                NotificationsService.create({
                  noti_type: Constants.notificationType.sms,
                  mobile: mobNum,
                  msg: message,
                  user_id: expdData.loc_id,
                  msg_id: transAlertStatusDoc.id,
                  msg_type: alertType,
                });
              })
            }
            if (expdData.comm_email) {
              NotificationsService.create({
                noti_type: Constants.notificationType.email,
                email: Constants.notificationMem.email,
                msg: message,
                user_id: expdData.loc_id,
                msg_id: transAlertStatusDoc.id,
                msg_type: alertType,
              });
            }
          }
        }
        trans_expanded_data_ids.push(expdData.exp_data_id)
      } catch (error) {
        winston.info(`Cron ACAlert error----->", ${error.stack}`);
      }
    }
    await expandeddata.update(
      { ac_alert_status: 1, updated_by: created_by },
      { where: { exp_data_id: { [Op.in]: trans_expanded_data_ids } } }
    );
    cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
  } catch (err) {
    console.error(err)
    cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
  } finally {
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  }
});

allcron.activeCommandId = new cron("0 */1 * * *", async () => {
  let data = await systemcontrol.findOne({ where: { cron_name: 'activeCommandId', run_cron_flag: 1, cronStatus: true } })
  if (!data) return winston.info("Cron activeCommandId error----->", "Cron flag set to 0 or cronStatus is false");

  await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
  let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "activeCommandId", status: "started", parent_table: "trans_command" });
  let created_by = "activeCommandId_" + cronEntry.id;
  // const t = await db.sequelize.transaction();
  try {
    const resData = findAll({ where: { is_active: Constants.active }, attributes: ['device_list', 'command'] })
    if (resData.length) {
      for (let i = 0; i < resData.length; i++) {
        global.devData.push({
          device_list: resData[i].deviceList,
          command: resData[i].command
        })
      }
    }

    cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
  } catch (err) {
    console.error(err)
    cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
  } finally {
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  }
});


allcron.daily_report = new cron("0 0 0 * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'daily_report', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "daily_report", status: "started", parent_table: "trans_expanded_data" });
    let created_by = "daily_report" + cronEntry.id;

    try {
      let now = moment().subtract(1, "days").format("YYYY-MM-DD")
      console.log("now", now);
      let expandeddataList = await db.sequelize.query(`
      select
        td.DID,max(ml.name) location,td.evt_dt, max(md.total_voltage) total_voltage,min(td.VB_min) VB_min,max(td.VB_max) VB_max,
         max(td.CAone_max) CAone_max,max(td.CAtwo_max) CAtwo_max,sum(ul.duration) duration,max(td.charg_max) charg_max,
         max(td.discharg_max) discharg_max,
        max(td.CS_max) CS_max,min(td.HAone_min) HAone_min, max(td.HAone_max) HAone_max, min(td.HAtwo_min) HAtwo_min,
        max(td.HAtwo_max) HAtwo_max, max(td.HS_max) HS_max,min(td.HS_min) HS_min,
        max(td.VN_max) VN_max,avg(td.TM) TM,min(td.raw_earth_min) raw_earth_min,max(td.VU_max) VU_max,
        max(ml.dev_status_last_recived) last_recived, max(td.raw_earth_max) raw_earth_max,min(td.ups_earth_min) ups_earth_min,
        max(td.ups_earth_max) ups_earth_max
      from mst_device md,mst_location ml,vw_trans_expanded_data_aggr td left join ups_logs ul on ul.device_id=td.DID 
      -- and date_format(ul.power_off, '%y-%m-%d')=date_format(evt_dt, '%y-%m-%d')
      and ul.power_off between  concat(CURDATE()- INTERVAL 1 DAY, " " , " 00:00:00") and concat(CURDATE()- INTERVAL 1 DAY , " " , " 23:59:59")
      where td.DID = md.dev_id
      and      --  td.evt_dt between  '2022-02-09 00:00:00' and '2022-02-09 23:59:59'
       td.evt_dt =  CURDATE()- INTERVAL 1 DAY 
      and md.dev_id=ml.dev_id and md.is_active =1 and ml.is_active =1
      GROUP BY td.DID, td.evt_dt 
            `, { type: db.sequelize.QueryTypes.SELECT });

      if (expandeddataList.length) {
        for (let i = 0; i < expandeddataList.length; i++) {
          const element = expandeddataList[i];
          console.log(element);
          const hAOneMin = currency(element.HAone_min, { precision: Constants.float_point }).value;

          const hAOneMax = currency(element.HAone_max, { precision: Constants.float_point }).value;

          const hATwoMin = currency(element.HAtwo_min, { precision: Constants.float_point }).value;

          const hATwoMax = currency(element.HAtwo_max, { precision: Constants.float_point }).value;

          let HS_cons = 0
          if ((element.HS_max - element.HS_min) > 0.001) {
            HS_cons = currency(element.HS_max, { precision: Constants.float_point }).subtract(element.HS_min).value
          }


          const newHourlyData = {
            DID: element.DID,
            location: element.location,
            duration: element.duration,
            battery: element.total_voltage,
            charging: element.charg_max,
            discharging: element.discharg_max,
            batt_low: element.VB_min,
            batt_high: element.VB_max,
            ac1_curr: element.CAone_max,
            ac2_curr: element.CAtwo_max,
            signage_curr: element.CS_max,
            last_recived: element.last_recived,
            temp: element.TM,
            vn_highest: element.VN_max,
            ups_out: element.VU_max,
            earthing: element.raw_earth_max,
            ups_earthing: element.ups_earth_max,
            ac1_kwh: currency(hAOneMax, { precision: Constants.float_point }).subtract(hAOneMin).value,
            ac2_kwh: currency(hATwoMax, { precision: Constants.float_point }).subtract(hATwoMin).value,
            sign_kwh: HS_cons,
            last_recived: element.last_recived,
            evt_dt: now,
            is_active: 1,
            created_by: created_by,
            updated_by: created_by,
          };

          const result = await dailyreport.findOrCreate({
            where: { DID: newHourlyData.DID, evt_dt: now },
            defaults: newHourlyData
          });

          if (!result[1]) {
            result[0].DID = newHourlyData.DID,
              result[0].location = newHourlyData.location,
              result[0].duration = newHourlyData.duration,
              result[0].battery = newHourlyData.total_voltage,
              result[0].charging = newHourlyData.charging,
              result[0].discharging = newHourlyData.discharging,
              result[0].batt_low = newHourlyData.batt_low,
              result[0].batt_high = newHourlyData.batt_high,
              result[0].ac1_curr = newHourlyData.ac1_curr,
              result[0].ac2_curr = newHourlyData.ac2_curr,
              result[0].signage_curr = newHourlyData.signage_curr,
              result[0].last_recived = newHourlyData.last_recived,
              result[0].temp = newHourlyData.temp,
              result[0].vn_highest = newHourlyData.vn_highest,
              result[0].ups_out = newHourlyData.ups_out,
              result[0].earthing = newHourlyData.earthing,
              result[0].ups_earthing = newHourlyData.ups_earthing,


              result[0].ac1_kwh = newHourlyData.ac1_kwh,
              result[0].ac2_kwh = newHourlyData.ac2_kwh,
              result[0].sign_kwh = newHourlyData.sign_kwh,
              result[0].updated_by = newHourlyData.created_by,
              await result[0].save();
            console.log(`hourlyRepo duplicate ${newHourlyData.DID} ${newHourlyData.evt_date_hr} `)
          }
        }
      }
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      winston.info(`Cron daily_report error-----> ${err.stack} `);
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    }
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  } else {
    winston.info("Cron daily_report error----->", "Cron flag set to 0 or cronStatus is false");
  }

});

allcron.checkCronStatus = new cron("0 */30 * * * *", async function () {
  let data = await systemcontrol.findOne({ where: { cron_name: 'checkCronStatus', run_cron_flag: 1, cronStatus: true } })
  if (data) {
    await systemcontrol.update({ cronStatus: false }, { where: { id: data.id } });
    let cronEntry = await cronmaintenenceObj.create({ start_time: new Date(), module: "checkCronStatus", status: "started", parent_table: "trans_expanded_data" });
    let created_by = "checkCronStatus" + cronEntry.id;

    try {
      let cronData = await db.sequelize.query(`select module, max(start_time) start_time, max(end_time)end_time
                                  from  cron_maintenence where  module in ("processRawData","processExpandedDataHr","prsExpandDataBy15Minutes") group by 1;

            `, { type: db.sequelize.QueryTypes.SELECT });

      if (cronData.length) {
        let flag = false
        let moduleName = { cronStatus: Constants.inactive };
        for (let i = 0; i < cronData.length; i++) {
          if (cronData[i].module == 'processRawData') {
            let start = moment(cronData[i].start_time) //start date
            let end = moment(cronData[i].end_time) // end date
            if (end < start) end = moment()
            let DIFF = moment.duration(end.diff(start));
            DIFF = DIFF.asMinutes()

            if (DIFF > 30) {
              moduleName.cron_name = "processRawData"
              flag = true
            }
          }
          else if (cronData[i].module == 'prsExpandDataBy15Minutes') {
            let start = moment(cronData[i].start_time); //start date
            let end = moment(cronData[i].end_time); // end date
            if (end < start) end = moment()
            let DIFF = moment.duration(end.diff(start));
            DIFF = DIFF.asMinutes()

            if (DIFF > 30) {
              moduleName.cron_name = "prsExpandDataBy15Minutes"
              flag = true
            }
          }
          else {
            let start = moment(cronData[i].start_time); //start date
            let end = moment(cronData[i].end_time); // end date
            if (end < start) end = moment()
            let DIFF = moment.duration(end.diff(start));
            DIFF = DIFF.asMinutes()

            if (DIFF > 30) {
              moduleName.cron_name = "processExpandedDataHr"
              flag = true
            }
          }
          if (flag) await systemcontrol.update({ cronStatus: Constants.active }, { where: moduleName })
        }
      }
      cronmaintenenceObj.update("success", new Date(), cronEntry.id, null, created_by)
    } catch (err) {
      winston.info(`Cron checkCronStatus error-----> ${err.stack} `);
      cronmaintenenceObj.update("failed", new Date(), cronEntry.id, err.stack, created_by)
    }
    systemcontrol.update({ cronStatus: true }, { where: { id: data.id } })
  } else {
    winston.info("Cron checkCronStatus error----->", "Cron flag set to 0 or cronStatus is false");
  }

});


// travant.in
// traveur.in

module.exports = allcron;

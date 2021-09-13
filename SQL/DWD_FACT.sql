/******************************************************订单明细事实表*****************************************************/
/*           时间   客户   PN   SN   Rma    Control   Returnto   OEM_Location   Service   FCode   度量值                 */
/* 订单详情    ✔     ✔    ✔    ✔     ✔        ✔         ✔             ✔          ✔        ✔     一件                  */
/**********************************************************************************************************************/
/*建表*/
select count(*) from dwd_fact_OrderDetail;
DROP TABLE IF EXISTS dwd_fact_OrderDetail;
CREATE TABLE dwd_fact_OrderDetail(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Area varchar(20),
    Year_Code varchar(10),
    Quarter_Code int,
    Month_Code int,
    Week_Code int,
    Order_ID int,
    Rma varchar(100),
    Con varchar(100) ,
    PPID varchar(100),
    PN varchar(20),
    SN varchar(100),
    CustomeID INT,
    ReturnTo varchar(30),
    OEM_Location varchar(30),
    ServiceID int,
    Commodity varchar(50),
    FCodeID int,
    fHawb varchar(20),
    fSRHawb varchar(30),
    CDate timestamp,
    Receive_Date timestamp,
    RequestRef varchar(20),
    ARB_YN varchar(5),
    Close_Date timestamp,
    OEMRma varchar(100),
    OEMRma_Date timestamp,
    Returnto_First varchar(30),
    RETURNTO_RMA varchar(100),
    Returnto_Last varchar(30),
    fRmaAssign_Date timestamp,
    updatetime timestamp default (now() - interval '1 day')
)Distributed by (ID);
create index dwd_fact_OrderDetail_index
	on dwd_fact_OrderDetail(id,updatetime,returnto);
/*注释*/
COMMENT on table dwd_fact_OrderDetail IS '订单明细事实表';
COMMENT on column dwd_fact_OrderDetail.ID IS '订单编号';
COMMENT on column dwd_fact_OrderDetail.Area IS '地区';
COMMENT on column dwd_fact_OrderDetail.Order_ID IS '订单ID';
COMMENT on column dwd_fact_OrderDetail.Rma IS '订单批号';
COMMENT on column dwd_fact_OrderDetail.OEMRma IS '客户批复RmaID';
COMMENT on column dwd_fact_OrderDetail.OEMRma_Date IS '客户批复Rma日期';
COMMENT on column dwd_fact_OrderDetail.fRmaAssign_Date IS '接收匹配日期';
COMMENT on column dwd_fact_OrderDetail.Con IS '货物ID';
COMMENT on column dwd_fact_OrderDetail.PPID IS '货物条码ID';
COMMENT on column dwd_fact_OrderDetail.PN IS '货物类别ID';
COMMENT on column dwd_fact_OrderDetail.SN IS 'SNID';
COMMENT on column dwd_fact_OrderDetail.CustomeID IS '客户ID';
COMMENT on column dwd_fact_OrderDetail.ReturnTo IS 'ReturnToID';
COMMENT on column dwd_fact_OrderDetail.OEM_Location IS '发往ID';
COMMENT on column dwd_fact_OrderDetail.Returnto_First IS '初始ReturnToID';
COMMENT on column dwd_fact_OrderDetail.CDate IS '订单创建日期';
COMMENT on column dwd_fact_OrderDetail.Receive_Date IS '订单接收日期';
COMMENT on column dwd_fact_OrderDetail.Close_Date IS '关单日期';
COMMENT on column dwd_fact_OrderDetail.ServiceID IS '业务ID';
COMMENT on column dwd_fact_OrderDetail.FCodeID IS '产品处理类型ID';
COMMENT on column dwd_fact_OrderDetail.fHawb IS '来货运单';
COMMENT on column dwd_fact_OrderDetail.fSRHawb IS '出货运单';
COMMENT on column dwd_fact_OrderDetail.updatetime IS '表更新时间';


/*插入数据*/
set enable_nestloop =off;
INSERT INTO dwd_fact_OrderDetail as dfOD (Order_ID, Rma, Con, PPID, PN, SN,ReturnTo,OEM_Location, CDate,Receive_Date
                                         ,RequestRef,Close_Date, OEMRma, OEMRma_Date, Returnto_First,RETURNTO_RMA
                                         ,Returnto_Last,Commodity,fRmaAssign_Date,FCodeID,fHawb,fSRHawb)
SELECT raid,frma,fcon,fppid,fpn,fsn,freturnto,fsendto,
       RMA.cdate,frptdate,frequestref,fclosedate,foemrma,
       fdate_oemrma,returnto_first,freturnto,Returnto_Last,
       bb.commodity,frmaassigndate,frescode,fhawb,fsrhawb
FROM ods_cj_rmaitem RMA
inner join ods_wx_returnto_base bb on RMA.fReturnTo = bb.ReturnTo
and bb.ServiceCode not in (1,9, 41, 60)
AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')  --, 'THB'
and Customer <> 'ZY'
WHERE RMA.updatetime::date=(now() - interval '1 day')::date
and RMA.fSta > 0;

select * from dwd_fact_orderdetail ;
/*UPDATE dwd_fact_orderdetail AS dfo
    SET CostomeID = customer from ods_wx_returnto_base AS owrb
        WHERE dfo.ReturnToID=owrb.returnto;*/
UPDATE dwd_fact_OrderDetail dfOD
SET Area=
    case
        when bb.CustomerCode = 'DELL ARB'
            then 'ARB'
        else 'XM'
        end
FROM ods_wx_returnto_base bb
WHERE dfOD.updatetime::date=(now() - interval '1 day')::date
      and dfOD.returnto=bb.returnto;

DROP TABLE IF EXISTS temp;
CREATE TEMPORARY TABLE temp(id int,customecode varchar(100),customer varchar(100),returnto varchar(30),serviceID int);
INSERT INTO temp(customecode,customer, returnto) SELECT customercode,customer,returnto  FROM ods_wx_returnto_base
WHERE updatetime::date=(now() - interval '1 day')::date;

UPDATE temp
    SET id= ddc.id FROM dwd_dim_custome  ddc
WHERE temp.customecode=ddc.customercode and temp.customer=ddc.customer;

UPDATE temp
    SET serviceID= owrb.servicecode FROM ods_wx_returnto_base AS owrb
WHERE temp.returnto=owrb.returnto;

UPDATE dwd_fact_OrderDetail dfOD
SET CustomeID = (select temp.id from temp where temp.returnto=dfOD.returnto)
WHERE dfOD.updatetime::date=(now() - interval '1 day')::date;

UPDATE dwd_fact_OrderDetail dfOD
SET ServiceID=(select temp.serviceID from temp where temp.returnto=dfOD.returnto)
WHERE updatetime::date=(now() - interval '1 day')::date;

update dwd_fact_OrderDetail dfOD set (Year_Code, Quarter_Code,Month_Code,Week_Code) = (bb.fym,bb.fqm,bb.fmm,bb.fwk)
from ods_holidays bb
where CDate=bb.fdate;

--排除HDD 未在SCREEN系统中的数据
DELETE FROM dwd_fact_OrderDetail
WHERE Order_ID IN
(
select AA.Order_ID From dwd_fact_OrderDetail AA
LEFT OUTER JOIN ods_cj_odditem bb on AA.Order_ID = bb.fItemid
 where Area = 'XM' and ReturnTo in ('CJHDDSC', 'RTHBTW', 'RTHBSSD')-- and Year_Code='FY19' AND Week_Code = 25
and bb.id is null
)
and updatetime::date=(now() - interval '1 day')::date;

--排除RMA CANCEL
delete  from  dwd_fact_OrderDetail where Order_ID in (select ID from ods_cj_rmaitem where fSta <=0)
and Area in ('XM', 'ARB') and updatetime::date=(now() - interval '1 day')::date;

--更新ARB YN 信息
update dwd_fact_OrderDetail
set ARB_YN = case when substring(RequestRef, 1,3) = 'ARB' then 'Y' else 'N' end
where ARB_YN is null and updatetime::date=(now() - interval '1 day')::date;

--排除 CJINUSE
delete from dwd_fact_OrderDetail where serviceid = 13
and RETURNTO_RMA = 'CJINUSE'
and SUBSTRING(Con, 1,4) ='CJIN'
and updatetime::date=(now() - interval '1 day')::date;

--FPD 排除CJINUSE 且上一次Returnto 为 CJSPHL  add by ruanjj/王克军  2021-2-1
delete  from dwd_fact_OrderDetail
where Area in ('XM','ARB')
and RETURNTO_RMA = 'CJINUSE'
and Returnto_First = 'CJSPHL'
and updatetime::date=(now() - interval '1 day')::date;

/*去重*/
DELETE FROM dwd_fact_OrderDetail
    WHERE ID in (SELECT a1.ID  FROM dwd_fact_OrderDetail a1,dwd_fact_OrderDetail a2
    where a1.con=a2.con and a1.updatetime::date<a2.updatetime::date);
/*****************************************************订单事实表（状态）拉链表***************************************************/
/*           时间   客户   PN   SN   Rma    Control   Returnto   OEM_Location   Service   FCode   度量值                 */
/* 订单状态    ✔                      ✔        ✔                                                  一件                  */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_OrderState;
CREATE TABLE dwd_fact_OrderState(
    Order_ID int,
    RmaID varchar(100),
    Con varchar(100) ,
    Order_stateID varchar(20),
    date_create timestamp,
    date_scrap timestamp,
    date_sr timestamp,
    date_OEM timestamp,
    date_sh timestamp,
    date_close timestamp,
    updatetime timestamp default (now() - interval '1 day')
/*订单生命周期：订单创建时间=>订单接收时间=>订单开始维修时间=>订单结束维修时间=>订单开始SR时间=>订单结束SR时间=>订单下架时间=>订单报废申请时间=>
  =>订单报废申请完成时间=>订单报废时间=>订单出货时间*/
)Distributed by (Order_ID);
/*注释*/
COMMENT on table dwd_fact_OrderState IS '订单状态表';
COMMENT on column dwd_fact_OrderState.Order_ID IS '订单编号ID';
COMMENT on column dwd_fact_OrderState.RmaID IS '订单RmaID';
COMMENT on column dwd_fact_OrderState.Con IS '货物ID';
COMMENT on column dwd_fact_OrderState.Order_stateID IS '订单状态ID';
COMMENT on column dwd_fact_OrderState.date_create IS '订单创建日期';
COMMENT on column dwd_fact_OrderState.date_scrap IS '订单报废日期';
COMMENT on column dwd_fact_OrderState.date_sr IS '订单对账日期';
COMMENT on column dwd_fact_OrderState.date_OEM IS '订单OEM请求日期';
COMMENT on column dwd_fact_OrderState.date_sh IS '订单出货日期';
COMMENT on column dwd_fact_OrderState.date_close IS '关单日期';
COMMENT on column dwd_fact_OrderState.updatetime IS '表更新日期';
/*插入数据*/
INSERT INTO dwd_fact_OrderState(Order_ID, RmaID, Con, Order_stateID, date_create, date_scrap, date_sr, date_OEM, date_sh, date_close)
SELECT raid,frma,fcon,fsta,cdate,fdate_scrap,fdate_sr,fdate_oemrma,fdate_sr,fclosedate FROM ods_cj_rmaitem
WHERE fsta IN (SELECT id FROM dwd_dim_Goodstate);


/******************************************************业务分流事实表*****************************************************/
/*           时间   客户   PN   SN   Rma    Control   Returnto   OEM_Location   Service   FCode   度量值                 */
/* 订单状态    ✔                      ✔        ✔                                                  一件                  */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Business_shunt;
CREATE TABLE dwd_fact_Business_shunt(
     ID INT,
     Con varchar(100)  NOT NULL,
     PPID varchar(100),
     Returnto_Original varchar(30),
     Returnto_Change varchar(30),
     Rma_Request varchar(100),
     Date_Received timestamp,
     Date_Shunt timestamp,
     updatetime timestamp default (now() - interval '1 day'),
     ID_FLAG INT NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1)
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_Business_shunt IS '业务分流事实表';
COMMENT on column dwd_fact_Business_shunt.ID IS '订单编号ID';
COMMENT on column dwd_fact_Business_shunt.Con IS '货物ID';
COMMENT on column dwd_fact_Business_shunt.PPID IS '货物条码ID';
COMMENT on column dwd_fact_Business_shunt.Returnto_Original IS '初始ReturnTO_ID';
COMMENT on column dwd_fact_Business_shunt.Returnto_Change IS '分流后ReturnTO_ID';
COMMENT on column dwd_fact_Business_shunt.Rma_Request IS '申请分流Rma_ID';
COMMENT on column dwd_fact_Business_shunt.Date_Received IS '产品接收日期';
COMMENT on column dwd_fact_Business_shunt.Date_Shunt IS '产品分流日期';
COMMENT on column dwd_fact_Business_shunt.updatetime IS '表更新日期';
/*插入数据*/
INSERT INTO dwd_fact_Business_shunt(ID, Con, PPID, Returnto_Original, Returnto_Change, Rma_Request, Date_Received, Date_Shunt)
SELECT id,fcon,fppid,returnto_from,returnto_to,requestrma,date_received,date_closed FROM ods_cj_rmachange_returnto
WHERE updatetime::date=(now() - interval '1 day')::date;
/*去重*/
DELETE FROM dwd_fact_Business_shunt
    WHERE ID_FLAG in (SELECT a1.ID_FLAG FROM dwd_fact_Business_shunt a1,dwd_fact_Business_shunt a2 where a1.con=a2.con and a1.updatetime<a2.updatetime);

select * from dwd_fact_Business_shunt where updatetime::date=(now() - interval '1 day')::date;
/*******************************************************报废事实表*******************************************************/
/*           时间   客户   PN   SN   Rma    Control   Returnto   OEM_Location   Service   FCode   度量值                 */
/* 报废       ✔                      ✔        ✔                                                  一件                  */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Scrap;
CREATE TABLE dwd_fact_Scrap(
    ID INT NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    Scrap_Con varchar(100),
    date_scrap timestamp,
    updatetime timestamp default (now() - interval '1 day')
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_Scrap IS '报废事实表';
COMMENT on column dwd_fact_Scrap.Order_ID IS '报废订单ID';
COMMENT on column dwd_fact_Scrap.RmaID IS '报废Rma号';
COMMENT on column dwd_fact_Scrap.Scrap_Con IS '报废货件号';
COMMENT on column dwd_fact_Scrap.date_scrap IS '报废日期';
/*插入数据*/
INSERT INTO dwd_fact_Scrap (Order_ID, RmaID,Scrap_Con,date_scrap)
SELECT scid,frma,fcon,fdate_scrap
FROM ods_cj_rmaitem
WHERE scid IS NOT NULL
AND updatetime::date=(now() - interval '1 day')::date;
select * from dwd_fact_Scrap;
/*去重*/
DELETE FROM dwd_fact_Scrap
    WHERE ID in (SELECT a1.id  FROM dwd_fact_Scrap a1,dwd_fact_Scrap a2 where a1.Scrap_Con=a2.Scrap_Con and a1.updatetime<a2.updatetime);
/******************************************************SR 对账事实表*****************************************************/
/*           时间   客户   PN   SN   Rma    Control   Returnto   OEM_Location   Service   FCode   度量值                 */
/* 对账       ✔                      ✔        ✔                                                  一件                  */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_SR_Accountchecking;
CREATE TABLE dwd_fact_SR_Accountchecking(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    Sr_Con varchar(100),
    date_srcheck timestamp,
    updatetime timestamp default (now() - interval '1 day')
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_SR_Accountchecking IS 'SR对账事实表';
COMMENT on column dwd_fact_SR_Accountchecking.Order_ID IS '订单ID';
COMMENT on column dwd_fact_SR_Accountchecking.RmaID IS '订单Rma号';
COMMENT on column dwd_fact_SR_Accountchecking.Sr_Con IS '对账货品Control号';
COMMENT on column dwd_fact_SR_Accountchecking.date_srcheck IS '对账时间';
/*数据插入*/
INSERT INTO dwd_fact_SR_Accountchecking (Order_ID, RmaID,Sr_Con,date_srcheck)
SELECT srid,frma,fcon,fdate_sr
FROM ods_cj_rmaitem
where fdate_sr IS NOT NULL
AND updatetime::date=(now() - interval '1 day')::date;
/*去重*/
DELETE FROM dwd_fact_SR_Accountchecking
    WHERE id in (SELECT a1.id  FROM dwd_fact_SR_Accountchecking a1,dwd_fact_SR_Accountchecking a2
    where a1.Sr_Con=a2.Sr_Con and a1.updatetime<a2.updatetime);
/****************************************************OEM RMA请求事实表***************************************************/
/*           时间   客户   PN   SN   Rma    Control   Returnto   OEM_Location   Service   FCode   度量值                 */
/* Rma请求    ✔                      ✔        ✔                                                  一件                  */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_OemRma_Apply;
CREATE TABLE dwd_fact_OemRma_Apply(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    OEM_Con varchar(100),
    date_OEM timestamp,
    updatetime timestamp default (now() - interval '1 day')
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_OemRma_Apply IS 'OEM RMA请求事实表';
COMMENT on column dwd_fact_OemRma_Apply.Order_ID IS 'OEM请求订单ID';
COMMENT on column dwd_fact_OemRma_Apply.RmaID IS 'OEM请求Rma号';
COMMENT on column dwd_fact_OemRma_Apply.OEM_Con IS 'OEM请求货号';
COMMENT on column dwd_fact_OemRma_Apply.date_OEM IS 'OEM请求日期';

/*数据插入*/
INSERT INTO dwd_fact_OemRma_Apply (Order_ID, RmaID,OEM_Con,date_OEM)
SELECT orid,frma,fcon,fdate_oemrma
FROM ods_cj_rmaitem
WHERE orid IS NOT NULL
AND updatetime::date=(now() - interval '1 day')::date;
/*去重*/
DELETE FROM dwd_fact_OemRma_Apply
    WHERE ID in (SELECT a1.id  FROM dwd_fact_OemRma_Apply a1,dwd_fact_OemRma_Apply a2
    where a1.OEM_Con=a2.OEM_Con and a1.updatetime<a2.updatetime);
/******************************************************SR 出货事实表*****************************************************/
/*           时间   客户   PN   SN   Rma   Returnto   OEM_Location   Service   FCode   度量值                            */
/* Rma批复    ✔                      ✔       ✔                                        一件                             */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_SR_Shipment;
CREATE TABLE dwd_fact_SR_Shipment(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    SH_Con varchar(100),
    OEM_LocationID varchar(30),
    date_sh timestamp,
    updatetime timestamp default (now() - interval '1 day')
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_SR_Shipment IS 'SR出货事实表';
COMMENT on column dwd_fact_SR_Shipment.Order_ID IS '出货订单ID';
COMMENT on column dwd_fact_SR_Shipment.RmaID IS '出货Rma';
COMMENT on column dwd_fact_SR_Shipment.SH_Con IS '出货货号';
COMMENT on column dwd_fact_SR_Shipment.date_sh IS '出货日期';
/*插入数据*/
set enable_nestloop =off;
INSERT INTO dwd_fact_SR_Shipment (Order_ID, RmaID, SH_Con, OEM_LocationID, date_sh)
SELECT shid,frma,fcon,fsendto,fclosedate
FROM ods_cj_rmaitem
WHERE shid IS NOT NULL
      AND updatetime::date=(now() - interval '1 day')::date;
/*去重*/
DELETE FROM dwd_fact_SR_Shipment
    WHERE ID in (SELECT a1.id  FROM dwd_fact_SR_Shipment a1,dwd_fact_SR_Shipment a2
    where a1.SH_Con=a2.SH_Con and a1.updatetime<a2.updatetime);

/******************************************************货品下架事实表*****************************************************/
/*           时间   客户   PN   SN   Rma   Returnto   OEM_Location   Service   FCode   度量值                            */
/* Rma批复    ✔                      ✔       ✔                                        一件                             */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Sold_Out;
CREATE TABLE dwd_fact_Sold_Out(
    ID INT PRIMARY KEY NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    fdate timestamp,
    updatetime timestamp default current_timestamp
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_Business_shunt IS '货品下架事实表';
COMMENT on column dwd_fact_Business_shunt.Con IS '货物ID';

/*数据插入*/
INSERT INTO dwd_fact_Sold_Out (Order_ID, RmaID, fdate)
SELECT id,frma FROM ods_cj_rmaitem

/*****************************************************货品报废申请事实表***************************************************/
/*           时间   客户   PN   SN   Rma   Returnto   OEM_Location   Service   FCode   度量值                            */
/* Rma批复    ✔                      ✔       ✔                                        一件                             */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Scrap_Apply;
CREATE TABLE dwd_fact_Scrap(
    ID INT PRIMARY KEY NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    fdate timestamp,
    updatetime timestamp default current_timestamp
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_Business_shunt IS '货品报废申请事实表';
COMMENT on column dwd_fact_Business_shunt.Con IS '货物ID';

/******************************************************好件出货事实表*****************************************************/
/*           时间   客户   PN   SN   Rma   Returnto   OEM_Location   Service   FCode   度量值                            */
/* Rma批复    ✔                      ✔       ✔                                        一件                             */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Shipment;
CREATE TABLE dwd_fact_Shipment(
    ID INT PRIMARY KEY NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    fdate timestamp,
    updatetime timestamp default current_timestamp
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_Business_shunt IS '好件出货事实表';
COMMENT on column dwd_fact_Business_shunt.Con IS '货物ID';



/******************************************************Rcp 出货事实表****************************************************/
/*           时间   客户   PN   SN   Rma   Returnto   OEM_Location   Service   FCode   度量值                            */
/* Rma批复    ✔                      ✔       ✔                                        一件                             */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Rcp_Shipment;
CREATE TABLE dwd_fact_Rcp_Shipment(
    ID INT PRIMARY KEY NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    fdate timestamp,
    updatetime timestamp default current_timestamp
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_Business_shunt IS 'Rcp出货事实表';
COMMENT on column dwd_fact_Business_shunt.Con IS '货物ID';

/****************************************************RTHBTW申请事实表****************************************************/
/*           时间   客户   PN   SN   Rma   Returnto   OEM_Location   Service   FCode   度量值                            */
/* Rma批复    ✔                      ✔       ✔                                        一件                             */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_RTHBTW_Apply;
CREATE TABLE dwd_fact_RTHBTW_Apply(
    ID INT PRIMARY KEY NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    Order_ID int,
    RmaID varchar(100),
    RTHBTH_Con varchar(100),
    date_RTHBTH timestamp,
    updatetime timestamp default (now() - interval '1 day')
)Distributed by (ID);
/*注释*/
COMMENT on table dwd_fact_RTHBTW_Apply IS 'RTHBTW申请事实表';
COMMENT on column dwd_fact_RTHBTW_Apply.Order_ID IS 'RTHBTH申请单号';
/*数据插入*/
INSERT INTO dwd_fact_SR_Shipment (Order_ID, RmaID, SH_Con)
SELECT rtid,frma,fcon FROM ods_cj_rmaitem WHERE shid IS NOT NULL ;




/******************************************************关单明细事实表*****************************************************/
/*           时间   客户   PN   SN   Rma    Control   Returnto   OEM_Location   Service   FCode   度量值                 */
/* 订单详情    ✔     ✔    ✔    ✔     ✔        ✔         ✔             ✔          ✔        ✔     一件                  */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Close_base_OrderDetail;
CREATE TABLE dwd_fact_Close_base_OrderDetail(
    ID int,
    Order_ID int,
    Area varchar(20),
    SvrCode int,
    Year_Code varchar(10),
    Quarter_Code int,
    Month_Code int,
    Week_Code int,
    Rma varchar(200),
    Con varchar(200) ,
    PPID varchar(200),
    PN varchar(20),
    SN varchar(100),
    CustomeID INT,
    Return_RMA varchar(100),
    ReturnTo varchar(30),
   -- Final_ReturnTo varchar(30),
    Commodity varchar(50),
    Model varchar(200),
    Model_Status varchar(200),
    Date_SafeLunch timestamp,
    RMA_issue_Date timestamp,
    Receive_Date timestamp,
    Start_Date timestamp,
    Packing_Date timestamp,
    Close_Date timestamp,
    TAT_Closed int,
    TAT_Packing int,
    TAT_sys int,
    Test_Result varchar(200),
    Final_Result varchar(200),
    NG_ErrorDesc varchar(200),
    NG_Reason varchar(200),
    ErrDesc_VFIR varchar(200),
    FailReason_VFIR varchar(200),
    Delay_Type varchar(200),
    Delay_Reason varchar(200),
    Delay_Category varchar(200),
    Submit_Date timestamp,
    Recover_Date timestamp,
    RequestRef varchar(200),
    Platform varchar(200),
    Return_times int,
    SO varchar(200),
    Service_tag varchar(200),
    ARB_YN varchar(10),
    SCRAP varchar(200),
    fResCode int,
    fsta int,
    fVIMFunRes int,
    fRepairRes int,
    IS_90RR varchar(10),
    LastID int,
    Last_Year_Code varchar(10),
    Last_Quarter_Code int,
    Last_Month_Code int,
    Last_Week_Code int,
    Last_Closed_Date timestamp,
    ImportantNote varchar(200),
    Last_ImportantNote varchar(200),
    Last_ServiceTAG varchar(20),
    Last_RequestREF varchar(200),
    Last_DelayCategory varchar(200),
    Last_Delaytype varchar(200),
    Last_Returnto varchar(200),
    Date_Bad_Post timestamp,
    Returnto_Last varchar(200),
    IsFakePart varchar(10),
    updatetime timestamp default (now() - interval '1 day'),
    ID_FLAG INT NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1)
)Distributed by (ID);

create index dwd_fact_close_base_orderdetail_id_index
	on dwd_fact_close_base_orderdetail (id,area,final_result,tat_closed,tat_packing,close_date);



/*插入数据*/

DROP VIEW IF EXISTS v_Screen_PN_Base;
CREATE VIEW v_Screen_PN_Base
AS
SELECT   fpn AS PN, fOem AS OEM, fVer AS MSR, fModel AS Model, frmaclass, fremark AS fjk, '' AS CLASS, fsta, fdesc,
fDate_Repair_Safe as Date_SafeLunch, -1 as Sta_ARB, null as Date_SafeLunch_ARB
FROM ods_cj_odd_pn where updatetime::date=(now() - interval '1 day')::date
UNION ALL
SELECT     fpn AS PN, fOem AS OEM, fVer AS MSR, fModel AS Model, frmaclass, fjk AS fjk, fclass AS Class, fsta, fdesc,
approveDate as Date_SafeLunch, -1 as Sta_ARB, null as Date_SafeLunch_ARB
FROM         ods_cj_hdd_pn where updatetime::date=(now() - interval '1 day')::date
UNION ALL
SELECT     fpn AS PN, '' AS OEM, '' AS MSR, fmodel AS Model, frmaclass, hmu AS fjk, fclass AS Class, fsta, fCustDesc AS fdesc,
fdate__safelunch as Date_SafeLunch, -1 as Sta_ARB, null as Date_SafeLunch_ARB
FROM         ods_cj_cpu_pn where updatetime::date=(now() - interval '1 day')::date
UNION ALL
SELECT     fpn AS PN, fOem AS OEM, fmsrver AS MSR, fModel AS Model, frmaclass, Standard_Accessary_Material AS fjk, fclass AS Class, fsta, fdesc,
fDate_Safe_Luanch  as Date_SafeLunch, -1 as Sta_ARB, null as Date_SafeLunch_ARB
FROM         ods_cj_repair_model where updatetime::date=(now() - interval '1 day')::date
UNION ALL
SELECT     fpn AS PN, fOem AS OEM, fver AS MSR, fModel AS Model, frmaclass, MSR_ARB AS fjk, '' AS Class, fsta, fdesc,
fDate_Repair_Safe as Date_SafeLunch, Sta_ARB, fDate_Repair_Safe_ARB Date_SafeLunch_ARB
FROM         ods_cj_mb_pn where updatetime::date=(now() - interval '1 day')::date
UNION ALL
SELECT     fpn AS PN, fOem AS OEM, fver AS MSR, fModel AS Model, frmaclass, cardbus_type AS fjk, fclass AS Class, fsta, fdesc,
ApproveDate  as Date_SafeLunch, -1 as Sta_ARB, null as Date_SafeLunch_ARB
FROM         ods_cj_card_pn where updatetime::date=(now() - interval '1 day')::date

/*
UNION ALL
SELECT     pn AS PN, 'Intel' AS OEM, MSR  AS MSR, Model AS Model,  52 AS frmaclass, BU  AS fjk,  Class, fsta, fdesc,
convert(date, Date_Repair_apply)  as Date_SafeLunch, -1 as Sta_ARB, null as Date_SafeLunch_ARB
FROM        ODM_PN_MB_Intel
*/
UNION ALL
SELECT     fpn AS PN, fOem AS OEM, fmsrver AS MSR, fModel AS Model, SvrCode as frmaclass, Platform AS fjk, Commodity AS Class, fsta, fdesc,
fDate_Safe_Luanch  as Date_SafeLunch, -1 as Sta_ARB, null as Date_SafeLunch_ARB
FROM         ods_base_pn_docking where updatetime::date=(now() - interval '1 day')::date;

/********************************************************插入数据********************************************************/

set enable_nestloop =off;
INSERT INTO dwd_fact_Close_base_OrderDetail(ID, Order_ID,Area, SvrCode, Year_Code, Quarter_Code, Month_Code,
                                       Week_Code, Rma, Con, PPID, PN, SN, Return_RMA,
                                       ReturnTo, Commodity, Model,Model_Status, Date_SafeLunch,
                                       RMA_issue_Date, Receive_Date, Start_Date, Packing_Date, Close_Date,
                                       TAT_Closed, TAT_Packing, Test_Result, Final_Result, NG_ErrorDesc,
                                       NG_Reason, ErrDesc_VFIR, FailReason_VFIR, Delay_Type, Delay_Reason,
                                       Delay_Category, Submit_Date, Recover_Date, RequestRef, Platform,
                                       Return_times, SO, Service_tag, SCRAP, fResCode, fsta, fVIMFunRes,
                                       fRepairRes, LastID, Last_Closed_Date, ImportantNote, Last_ImportantNote,
                                       Last_ServiceTAG, Last_RequestREF, Last_DelayCategory, Last_Delaytype,
                                       Last_Returnto, Date_Bad_Post, Returnto_Last)
SELECT id,Order_ID,Area,SvrCode,Year_Code,Quarter_Code,Month_Code,Week_Code,RMA,Control,PPID,PN,SN,RETURNTO_RMA,RETURNTO,
       Commodity,Model,Model_Status,Date_SafeLunch,RMA_issue_Date,Receive_Date,Start_Date,Packing_Date,Close_Date,TAT_Closed,
       TAT_Packing,Test_Result,Final_Result, NG_ErrorDesc,NG_Reason, ErrDesc_VFIR, FailReason_VFIR, Delay_Type, Delay_Reason,
       Delay_Category, Submit_Date, Recover_Date, RequestRef, Platform,Return_times, SO, Service_tag, SCRAP, fResCode, fsta,
       fVIMFunRes,fRepairRes, LastID, Last_Closed_Date, ImportantNote, Last_ImportantNote,Last_ServiceTAG, Last_RequestREF,
       DelayCategory, Delaytype,Last_Returnto, Date_Bad_Post, Returnto_Last FROM
(select aa.fItemid as Order_ID,
case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' end as Area,
bb.ServiceCode as SvrCode, aa.id, cc.fym Year_Code , cc.fqm as Quarter_Code, cc.fmm Month_Code , cc.fwk Week_Code,
rma.fReturnTo as RETURNTO_RMA, aa.fReturnto as RETURNTO, bb.Commodity AS Commodity,
RMA.fCon as Control, RMA.frma as RMA, aa.fpn as PN, aa.fppid AS PPID,   aa.fsn as SN, dd.Model as Model,
case when aa.fRmaClass = 45 then --ARB MB
case when dd.Sta_ARB in (2,6) THEN 'Safe Launch'  else 'NPI' end
else
case when dd.fsta in (2,6) THEN 'Safe Launch'  else 'NPI' end
end as Model_Status, --a1.FCodeName

case when aa.fRmaClass = 45 then dd.Date_SafeLunch_arb
else Date_SafeLunch end  as Date_SafeLunch,
RMA.Cdate as RMA_issue_Date,
RMA.fRptDate as Receive_Date, to_timestamp(null::text,'YYYY-MM-dd hh:mm:ss[.f...]') as Start_Date,
case when aa.fsta = 19 then aa.fDate_Pack else aa.fDate_BadPost end AS Packing_Date,
aa.fShipDate as Close_Date , -1 as TAT_Closed  , -1 as TAT_Packing,
case when ee.Disposition <> '' then ee.Disposition else  a2.FCodeName  end as Test_Result,
''  Final_Result,
case when aa.fsta = 29 and aa.frescode <>1 then aa.fErrDesc_En  ELSE '' end as NG_ErrorDesc,
case when aa.fsta = 29 and aa.frescode <>1 then aa.fFailReasen_En  ELSE '' end as NG_Reason,
ee.fErrDesc as ErrDesc_VFIR, ee.fFailReason FailReason_VFIR,
f1.fCodeName as Delay_Type  , ff.fWaitReason Delay_Reason,
ff.fRemark Delay_Category, ff.Cdate as Submit_Date,ff.fUnWaitDate  as Recover_Date,
RMA.fRequestREF as RequestRef, dd.model as Platform,
aa.fReturnTimes Return_times, rma.fSo as SO, RMA.fSt as Service_tag,
'' AS SCRAP,
aa.fResCode , aa.fsta, aa.fVIMFunRes, aa.fRepairRes,
aa.fLastID as LastID, RR.fShipDate as Last_Closed_Date, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote,
RMA_RR.fSt as  Last_ServiceTAG, RMA_RR.fRequestREF as  Last_RequestREF,
f1.DelayCategory, f1.Delaytype, RMA_RR.fReturnTo as Last_Returnto, aa.fDate_BadPost as Date_Bad_Post,
RMA.Returnto_Last


from ods_cj_odditem aa
inner join ods_cj_rmaitem RMA on RMA.Id = aa.fItemid
inner join ods_wx_returnto_base bb on RMA.fReturnTo = bb.ReturnTo  AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA','THB')
left outer join ods_holidays cc on aa.fShipDate = to_timestamp(cc.fsDate::text,'YYYY-MM-dd hh:mm:ss[.f...]')
left outer join v_Screen_PN_Base dd on dd.PN = aa.fpn
--left outer join cj_code a1 on a1.FType = 56 and a1.FCode = dd.fsta
left outer join ods_screen_disposition ee on ee.fScreenID = aa.id
left outer join ods_wx_wait_master ff on ff.id = aa.fwaitid
left outer join ods_cj_delay_mast f1 on ff.fWaitType = f1.fcode
LEFT OUTER JOIN ods_cj_code a2 ON a2.FType = 22 AND a2.FParent = '7' AND a2.FCode = aa.fVIMFunRes
left outer join ods_cj_odditem RR on RR.id = aa.fLastID
left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.fItemid

where aa.fsta IN (19,29)
and RMA.fReturnTo in (select ReturnTo from ods_wx_returnto_base where  CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA','THB'))
AND bb.ServiceCode in (8, 14, 16,17,18,  21,22,23, 25,26,27,28,32,36,39,43,44,45, 87)
and aa.fRmaClass <> 46
and aa.fShipDate >= '2020-03-01'
and aa.updatetime::date=(now() - interval '1 day')::date

UNION ALL

select aa.fItemid as Order_ID,
case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' end as Area,
bb.ServiceCode as SvrCode, aa.id, cc.fym Year_Code ,cc.fqm as Quarter_Code, cc.fmm Month_Code , cc.fwk Week_Code,
rma.fReturnTo as RETURNTO_RMA, aa.fReturnto as RETURNTO, bb.Commodity,
RMA.fCon as Control, RMA.frma as RMA, aa.fpn as PN, aa.fppid AS PPID,   aa.fsn as SN, dd.Model,
case when dd.fsta in (2,6) THEN 'Safe Launch'  else 'NPI' end as Model_Status, --a1.FCodeName
dd.fDate_Repair_Safe, to_date(RMA.Cdate::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as RMA_issue_Date, to_date(RMA.fRptDate::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Receive_Date,
to_date(null::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Start_Date,
case when aa.fsta = 19 then to_date(aa.fDate_Pack::text,'YYYY-MM-dd HH24:MI:SS[.f...]') else to_date(aa.fDate_BadPost::text,'YYYY-MM-dd HH24:MI:SS[.f...]') end AS Packing_Date,
aa.fShipDate as Close_Date , -1 as TAT_Closed  , -1 as TAT_Packing,
case when ee.Disposition <> '' then ee.Disposition else  a2.FCodeName  end as Test_Result,
'' Final_Result,
case when aa.fsta = 29 and aa.frescode <>1 then aa.fErrDesc_En  ELSE '' end as NG_ErrorDesc,
case when aa.fsta = 29 and aa.frescode <>1 then aa.fFailReasen_En  ELSE '' end as NG_Reason,
ee.fErrDesc as ErrDesc_VFIR, ee.fFailReason FailReason_VFIR,
f1.fCodeName as Delay_Type  , ff.fWaitReason Delay_Reason,
ff.fRemark Delay_Category, ff.Cdate as Submit_Date,ff.fUnWaitDate as Recover_Date,
RMA.fRequestREF as RequestRef, dd.model as Platform,
aa.fReturnTimes Return_times, rma.fSo as SO, RMA.fSt as Service_tag ,
--CASE WHEN aa.frescode = 1 THEN 'No' WHEN aa.frescode = 2 THEN ee.scrap ELSE '' END
'' AS SCRAP,
aa.fResCode , aa.fsta, aa.fVIMFunRes, aa.fRepairRes,
aa.fLastID as LastID, RR.fShipDate as Last_Closed_Date, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote,
RMA_RR.fSt as  Last_ServiceTAG,  RMA_RR.fRequestREF as  Last_RequestREF,
f1.DelayCategory, f1.Delaytype, RMA_RR.fReturnTo as Last_Returnto, aa.fDate_BadPost as Date_Bad_Post,
RMA.Returnto_Last
--case when RR.fShipDate <> '' then DATEDIFF(DAY, RR.fShipDate, RMA.fRptDate) else 0 end AS Days_RR
from ods_cj_oddItem aa
left outer join ods_cj_rmaitem RMA on RMA.Id = aa.fItemid
inner join ods_wx_returnto_base bb on RMA.fReturnTo = bb.ReturnTo AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')
left outer join ods_holidays cc on aa.fShipDate = to_timestamp(cc.fsDate::text,'YYYY-MM-dd hh:mm:ss[.f...]')
left outer join dwd_dim_mem dd on dd.pn = aa.fpn and aa.ODM = dd.odm
--left outer join cj_code a1 on a1.FType = 56 and a1.FCode = dd.fsta
left outer join ods_screen_disposition ee on ee.fScreenID = aa.id
left outer join ods_wx_wait_master ff on ff.id = aa.fwaitid
left outer join ods_cj_delay_mast f1 on ff.fWaitType = f1.fcode
LEFT OUTER JOIN ods_cj_code a2 ON a2.FType = 22 AND a2.FParent = '7' AND a2.FCode = aa.fVIMFunRes
left outer join ods_cj_odditem RR on RR.id = aa.fLastID
left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.fItemid
where aa.fsta IN (19,29) AND bb.ServiceCode in (15,24)
and RMA.fReturnTo in (select ReturnTo from ods_wx_returnto_base where  CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA','THB'))
and aa.fShipDate >= '2020-01-01'
and aa.updatetime::date=(now() - interval '1 day')::date
)as temp;
/*************************************************更新scrap 和 testresult************************************************/
DROP TABLE IF EXISTS temp_screen;
CREATE TEMPORARY TABLE temp_screen(id int,Scrap varchar(30),fsta int);
INSERT INTO temp_screen(id, Scrap,fsta)
SELECT fScreenID,scrap,fsta  FROM ods_screen_disposition
where fsta in (19,29) and updatetime::date=(now() - interval '1 day')::date;

select count(*) from temp_screen;

UPDATE dwd_fact_Close_base_OrderDetail aa
SET SCRAP=cj_KPI_DELL_IsScrap(aa.returnto, aa.SvrCode, aa.fsta, aa.frescode, aa.fVIMFunRes, aa.fRepairRes,
       CASE WHEN aa.frescode = 1 THEN 'No' WHEN aa.frescode = 2 THEN temp_screen.scrap WHEN aa.frescode = 3 THEN 'RTV' ELSE '' END)
FROM   temp_screen
WHERE aa.id=temp_screen.id and
      aa.SvrCode in (8, 14, 15, 16,17,18,  21,22,23, 24, 25,26,27,28,32,36,39,43,44,45, 87)
    -- and  aa.Commodity not in  ('DT LCD' , 'Monitor')
     and aa.Area in ('ARB', 'XM')
     and updatetime::date=(now() - interval '1 day')::date
  --  and aa.fResCode in (1,2,3)
    and COALESCE(aa.Scrap, '') <> cj_KPI_DELL_IsScrap(aa.Returnto, aa.SvrCode, aa.fsta, aa.frescode, aa.fVIMFunRes, aa.fRepairRes,
      CASE WHEN aa.frescode = 1 THEN 'No' WHEN aa.frescode = 2 THEN temp_screen.scrap WHEN aa.frescode = 3 then 'RTV' ELSE '' END);
set enable_nestloop =off;
update dwd_fact_Close_base_OrderDetail aa
    set Test_Result = bb.Disposition
   -- select aa.*
    from ods_screen_disposition bb
    where aa.updatetime::date=(now() - interval '1 day')::date
    and bb.fScreenID = aa.id and bb.fsta in (19,29)
    and   aa.SvrCode in (8, 14, 15, 16,17,18,  21,22,23, 24, 25,26,27,28,32,36,39,43,44,45, 87)
    -- aa.Commodity not in  ('DT LCD' , 'Monitor')
    and aa.Area in ('ARB', 'XM')
    and COALESCE(aa.Test_Result, '') <> COALESCE(bb.Disposition, '');

SELECT Dell_KPI_days('dwd_fact_close_base_orderdetail');--更新其他
/*去重*/
DELETE FROM dwd_fact_Close_base_OrderDetail
    WHERE ID_FLAG in (SELECT a1.ID_FLAG  FROM dwd_fact_Close_base_OrderDetail a1,dwd_fact_Close_base_OrderDetail a2
    where a1.Con=a2.Con and a1.updatetime<a2.updatetime);
/***************************************************monitor关单事实表****************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_fact_Close_monitor_OrderDetail;
CREATE TABLE dwd_fact_Close_monitor_OrderDetail(
    ID int,
    Order_ID int,
    Area varchar(20),
    SvrCode int,
    Year_Code varchar(10),
    Quarter_Code int,
    Month_Code int,
    Week_Code int,
    Rma varchar(200),
    Con varchar(200) ,
    PPID varchar(200),
    PN varchar(20),
    SN varchar(100),
    CustomeID INT,
    Return_RMA varchar(100),
    ReturnTo varchar(30),
   -- Final_ReturnTo varchar(30),
    Commodity varchar(50),
    Model varchar(200),
    Model_Status varchar(300),
    Date_SafeLunch timestamp,
    RMA_issue_Date timestamp,
    Receive_Date timestamp,
    Start_Date timestamp,
    Packing_Date timestamp,
    Close_Date timestamp,
    TAT_Closed int,
    TAT_Packing int,
    TAT_sys int,
    Test_Result varchar(300),
    Final_Result varchar(300),
    NG_ErrorDesc varchar(300),
    NG_Reason varchar(300),
    ErrDesc_VFIR varchar(300),
    FailReason_VFIR varchar(300),
    Panel_Reapir varchar(20),
    Panel_Repair_type varchar(100),
    Delay_Type varchar(200),
    Delay_Reason varchar(300),
    Delay_Category varchar(200),
    Submit_Date timestamp,
    Recover_Date timestamp,
    RequestRef varchar(200),
    Platform varchar(200),
    Return_times int,
    SO varchar(200),
    Service_tag varchar(200),
    ARB_YN varchar(10),
    SCRAP varchar(200),
    fResCode int,
    fsta int,
    fVIMFunRes int,
    fRepairRes int,
    IS_90RR varchar(10),
    LastID int,
    Last_Year_Code varchar(10),
    Last_Quarter_Code int,
    Last_Month_Code int,
    Last_Week_Code int,
    Last_Closed_Date timestamp,
    ImportantNote varchar(300),
    Last_ImportantNote varchar(300),
    Last_ServiceTAG varchar(20),
    Last_RequestREF varchar(300),
    Last_DelayCategory varchar(200),
    Last_Delaytype varchar(200),
    Last_Returnto varchar(200),
    Date_Bad_Post timestamp,
    Returnto_Last varchar(200),
    IsFakePart varchar(10),
    updatetime timestamp default now() - interval '1 day',
    ID_FLAG INT NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1)
)Distributed by (ID);
create index dwd_fact_Close_monitor_OrderDetail_id_index
	on dwd_fact_Close_monitor_OrderDetail(id,area,final_result,tat_closed,tat_packing,close_date);
/********************************************************插入数据********************************************************/
INSERT INTO dwd_fact_Close_monitor_OrderDetail(ID, Order_ID, Area, SvrCode, Year_Code, Quarter_Code, Month_Code, Week_Code, Rma,
                                               Con, PPID, PN, SN, Return_RMA, ReturnTo, Commodity, Model, Model_Status,
                                               Date_SafeLunch, RMA_issue_Date, Receive_Date, Start_Date, Packing_Date, Close_Date,
                                               TAT_Closed, TAT_Packing, Test_Result, Final_Result, NG_ErrorDesc, NG_Reason, ErrDesc_VFIR,
                                               FailReason_VFIR, Panel_Reapir, Panel_Repair_type, Delay_Type, Delay_Reason, Delay_Category,
                                               Submit_Date, Recover_Date, RequestRef, Platform, Return_times, SO, Service_tag, SCRAP, fResCode,
                                               fsta, fVIMFunRes, fRepairRes, LastID, Last_Closed_Date, ImportantNote, Last_ImportantNote, Last_ServiceTAG,
                                               Last_RequestREF, Last_DelayCategory, Last_Delaytype, Last_Returnto, Date_Bad_Post, Returnto_Last)
    SELECT id,Order_ID,Area,SvrCode,Year_Code,Quarter_Code,Month_Code,Week_Code,RMA,Control,PPID,PN,SN,RETURNTO_RMA,RETURNTO,
        Commodity,Model,Model_Status,Date_SafeLunch,RMA_issue_Date,Receive_Date,Start_Date,Packing_Date,Close_Date,TAT_Closed,
        TAT_Packing,Test_Result,Final_Result, NG_ErrorDesc,NG_Reason, ErrDesc_VFIR, FailReason_VFIR,Panel_Reapir,Panel_Repair_type, Delay_Type,Delay_Reason,
        Delay_Category, Submit_Date, Recover_Date, RequestRef, Platform,Return_times, SO, Service_tag, SCRAP, fResCode, fsta,
        fVIMRes,fRepairRes, LastID, Last_Closed_Date, ImportantNote, Last_ImportantNote,Last_ServiceTAG, Last_RequestREF,
        DelayCategory, Delaytype,Last_Returnto, Date_Bad_Post, Returnto_Last FROM
    (select aa.fItemid as Order_ID,
    case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' end as Area,
    bb.servicecode as SvrCode, aa.id, cc.fym Year_Code , cc.fqm as Quarter_Code, cc.fmm Month_Code , cc.fwk Week_Code,
    rma.fReturnTo as RETURNTO_RMA, aa.fReturnto as RETURNTO, bb.Commodity,
    RMA.fCon as Control, RMA.frma as RMA, aa.fpn as PN, aa.fppid AS PPID,  rma.fSn as SN, dd.Platform as Model,
    case when fn_FPD_PN_fmsta(RMA.fReturnto,dd.fmSta,dd.fsta_ARB) in (2,6) THEN 'Safe Launch'  else 'NPI' end as Model_Status, --a1.FCodeName
    fn_FPD_PN_ApproveDate(RMA.fReturnto,dd.ApproveDate,dd.Date_SafeLunch_ARB) as Date_SafeLunch, RMA.Cdate as RMA_issue_Date,
    RMA.fRptDate as Receive_Date, to_date(null::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Start_Date,
    case when aa.fsta = 59 then aa.fDate_FinalTest else to_timestamp(aa.fDate_Post::text,'YYYY-MM-dd HH24:MI:SS[.f...]') end AS Packing_Date,
    to_timestamp(aa.fDate_Close::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Close_Date , -1 as TAT_Closed  , -1 as TAT_Packing,
    --case when ee.Disposition <> '' then ee.Disposition else  a2.FCodeName  end
    '' as Test_Result,
    '' as Final_Result,
    case when aa.fsta IN (39,49)  then aa.fErrDesc  ELSE '' end as NG_ErrorDesc,
    case when aa.fsta IN (39,49)  then aa.fErrDesc  ELSE '' end as NG_Reason,
    ee.fErrDesc as ErrDesc_VFIR, ee.fFailReason FailReason_VFIR,
    case when PP.id > 0 then 'Y' else 'N' end as Panel_Reapir,  a7.FCodeName Panel_Repair_type,
    f1.fCodeName as Delay_Type  , ff.fWaitReason Delay_Reason,
    ff.fRemark Delay_Category, ff.Cdate as Submit_Date, ff.fUnWaitDate as Recover_Date,
    RMA.fRequestREF as RequestRef, dd.Platform as Platform,
    aa.fReTimes Return_times, rma.fSo as SO, RMA.fSt as Service_tag ,
    ee.Scrap as Scrap,--ee.Scrap,
    aa.fResCode , aa.fsta, aa.fVIMRes, aa.fRepairRes,
    aa.fLastID as LastID, to_timestamp(RR.fDate_Close::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Last_Closed_Date, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote,
    RMA_RR.fSt as  Last_ServiceTAG,  RMA_RR.fRequestREF as  Last_RequestREF,
    f1.DelayCategory, f1.Delaytype, RMA_RR.fReturnTo as Last_Returnto, to_timestamp(aa.fDate_Close::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Date_Bad_Post,
    RMA.Returnto_Last
    --case when RR.fDate_Close <> '' then DATEDIFF(DAY, RR.fDate_Close, RMA.fRptDate) else 0 end AS Days_RR
    from ods_wx_item aa
    inner join ods_cj_rmaitem RMA on RMA.Id = aa.fItemid
    left outer join ods_odm_rma_item PP on PP.id = aa.NewPanel_ID and PP.SvrCode = 202
    inner join ods_wx_returnto_base bb on RMA.fReturnTo = bb.ReturnTo AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')
    left outer join ods_Holidays cc on aa.fDate_Close = cc.fsDate
    left outer join ods_wx_monitor dd on dd.fPN = aa.fpn
    --left outer join cj_code a1 on a1.FType = 56 and a1.FCode = dd.fmSta
    left outer join ods_wx_disposition ee on ee.fwxID = aa.id
    left outer join ods_wx_wait_master ff on ff.id = aa.fwaitid
    left outer join ods_cj_delay_mast f1 on ff.fWaitType = f1.fcode
    LEFT OUTER JOIN ods_cj_code a2 ON a2.FType = 22 AND a2.FParent = '7' AND a2.FCode = aa.fVIMRes
    left outer join ods_cj_panel_code a7 on a7.FType = 23 and a7.FParent = '1' and a7.FCode = PP.PanelRepairType
    left outer join ods_wx_item RR on RR.id = aa.fLastID
    left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.fItemid
    where  aa.fsta IN (19,34,39,49,59,69,51,52,53) and RMA.fReturnTo in ('CJREPAIR', 'CJINUSE', 'ARBCJMON') -- and bb.ServiceCode in (13)
    and COALESCE(aa.fDate_Close, '') >= '2020-01-01'
    and aa.updatetime::date=(now() - interval '1 day')::date
    )as temp;
/*************************************************更新scrap 和 testresult************************************************/
DROP TABLE IF EXISTS temp_monitor;
CREATE TEMPORARY TABLE temp_monitor(id int,Scrap varchar(30),Disposition varchar(5));
INSERT INTO temp_monitor(id, Scrap, Disposition)
SELECT fwxid,scrap,disposition  FROM ods_wx_disposition as owd
where fsta in (19,34,39,49,51,52,53,59) and updatetime::date=(now() - interval '1 day')::date;

UPDATE dwd_fact_Close_monitor_OrderDetail dfCmOD
SET SCRAP=temp_monitor.Scrap,Test_Result = temp_monitor.Disposition
FROM temp_monitor
WHERE dfCmOD.id=temp_monitor.id and dfCmOD.SvrCode=13  and dfCmOD.Area in ('ARB', 'XM')
and ( COALESCE(dfCmOD.SCRAP, '') <> COALESCE(temp_monitor.SCRAP, '')
        or COALESCE(dfCmOD.Test_Result, '') <> COALESCE(temp_monitor.Disposition, ''))
and dfCmOD.updatetime::date=(now() - interval '1 day')::date;

update dwd_fact_Close_monitor_OrderDetail aa
     set SCRAP = bb.Scrap,Test_Result = bb.Disposition
     from ods_wx_disposition bb
     where aa.SvrCode = 13  and aa.Area in ('ARB', 'XM')
     and bb.fwxID = aa.id and bb.fsta in (19,34,39,49,51,52,53,59)
     and ( COALESCE(aa.SCRAP, '') <> COALESCE(bb.SCRAP, '')
        or COALESCE(aa.Test_Result, '') <> COALESCE(bb.Disposition, '')   )
     and aa.updatetime::date=(now() - interval '1 day')::date;

SELECT Dell_KPI_days('dwd_fact_close_monitor_orderdetail');--更新其他

/*去重*/
DELETE FROM dwd_fact_close_monitor_orderdetail
    WHERE ID_FLAG in (SELECT a1.ID_FLAG  FROM dwd_fact_close_monitor_orderdetail a1,dwd_fact_close_monitor_orderdetail a2
    where a1.Con=a2.Con and a1.updatetime<a2.updatetime);
/***************************************************panel关单事实表****************************************************/
DROP TABLE IF EXISTS dwd_fact_Close_panel_OrderDetail;
CREATE TABLE dwd_fact_Close_panel_OrderDetail(
    ID int,
    Order_ID int,
    Area varchar(20),
    SvrCode int,
    Year_Code varchar(10),
    Quarter_Code int,
    Month_Code int,
    Week_Code int,
    Rma varchar(200),
    Con varchar(200) ,
    PPID varchar(200),
    PN varchar(20),
    SN varchar(100),
    CustomeID INT,
    Return_RMA varchar(100),
    ReturnTo varchar(30),
   -- Final_ReturnTo varchar(30),
    Commodity varchar(50),
    Model varchar(200),
    Model_Status varchar(300),
    Date_SafeLunch timestamp,
    RMA_issue_Date timestamp,
    Receive_Date timestamp,
    Start_Date timestamp,
    Packing_Date timestamp,
    Close_Date timestamp,
    TAT_Closed int,
    TAT_Packing int,
    TAT_sys int,
    Test_Result varchar(300),
    Final_Result varchar(300),
    NG_ErrorDesc varchar(300),
    NG_Reason varchar(300),
    ErrDesc_VFIR varchar(300),
    FailReason_VFIR varchar(300),
    Panel_Reapir varchar(20),
    Panel_Repair_type varchar(100),
    Delay_Type varchar(200),
    Delay_Reason varchar(300),
    Delay_Category varchar(200),
    Submit_Date timestamp,
    Recover_Date timestamp,
    RequestRef varchar(200),
    Platform varchar(200),
    Return_times int,
    SO varchar(200),
    Service_tag varchar(200),
    ARB_YN varchar(10),
    SCRAP varchar(200),
    fResCode int,
    fsta int,
    fVIMFunRes int,
    fRepairRes int,
    IS_90RR varchar(10),
    LastID int,
    Last_Year_Code varchar(10),
    Last_Quarter_Code int,
    Last_Month_Code int,
    Last_Week_Code int,
    Last_Closed_Date timestamp,
    ImportantNote varchar(300),
    Last_ImportantNote varchar(300),
    Last_ServiceTAG varchar(20),
    Last_RequestREF varchar(300),
    Last_DelayCategory varchar(200),
    Last_Delaytype varchar(200),
    Last_Returnto varchar(200),
    Date_Bad_Post timestamp,
    Returnto_Last varchar(200),
    IsFakePart varchar(10),
    updatetime timestamp default now() - interval '1 day',
    ID_FLAG INT NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1)
)Distributed by (ID);
create index dwd_fact_Close_panel_OrderDetail_index
	on dwd_fact_Close_panel_OrderDetail(id,area,final_result,tat_closed,tat_packing,close_date);
/*******************************插入数据**************************************/
set enable_nestloop =off;
INSERT INTO dwd_fact_Close_panel_OrderDetail(  ID, Area, SvrCode, Year_Code, Quarter_Code, Month_Code, Week_Code, Rma,
                                               Con, PPID, PN, SN, Return_RMA, ReturnTo, Commodity, Model_Status,
                                               Date_SafeLunch, RMA_issue_Date, Receive_Date, Start_Date, Packing_Date, Close_Date,
                                               TAT_Closed, TAT_Packing, Test_Result, Final_Result, NG_ErrorDesc, NG_Reason, ErrDesc_VFIR,
                                               FailReason_VFIR, Panel_Reapir, Panel_Repair_type, Delay_Type, Delay_Reason, Delay_Category,
                                               Submit_Date, Recover_Date, RequestRef, Platform, Return_times, SO, Service_tag, SCRAP, fResCode,
                                               fsta, fVIMFunRes, LastID, Last_Closed_Date, ImportantNote, Last_ImportantNote, Last_ServiceTAG,
                                               Last_RequestREF, Last_DelayCategory, Last_Delaytype, Last_Returnto, Date_Bad_Post, Returnto_Last)
SELECT id,Area,SvrCode,Year_Code,Quarter_Code,Month_Code,Week_Code,RMA,Control,PPID,PN,SN,RETURNTO_RMA,RETURNTO,
    Commodity,Model_Status,Date_SafeLunch,RMA_issue_Date,Receive_Date,Start_Date,Packing_Date,Close_Date,TAT_Closed,
    TAT_Packing,Test_Result,Final_Result, NG_ErrorDesc,NG_Reason, ErrDesc_VFIR, FailReason_VFIR,Panel_Reapir,Panel_Repair_type, Delay_Type,Delay_Reason,
    Delay_Category, Submit_Date, Recover_Date, RequestRef, Platform,Return_times, SO, Service_tag, SCRAP, fResCode, fsta,
    fVIMRes, LastID, Last_Closed_Date, ImportantNote, Last_ImportantNote,Last_ServiceTAG, Last_RequestREF,
    DelayCategory, Delaytype,Last_Returnto, Date_Bad_Post, Returnto_Last FROM
    (select
    case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' end as Area,
    pp.SvrCode, PP.id, cc.fym Year_Code , cc.fqm as Quarter_Code, cc.fmm Month_Code , cc.fwk Week_Code,
    rma.fReturnTo as RETURNTO_RMA, PP.Returnto as RETURNTO, bb.Commodity,
    RMA.fCon as Control, RMA.frma as RMA, PP.fpn as PN, PP.MonitorPPID AS PPID, PP.PPID as SN, dd.fModel as Modal, --rma.fSn as SN
    case when dd.fsta in (6) THEN 'Safe Launch'  else 'NPI' end as Model_Status, --a1.FCodeName
    dd.Date_SafeLunch as Date_SafeLunch, RMA.Cdate as RMA_issue_Date,
    RMA.fRptDate as Receive_Date, to_date(null::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Start_Date,
    PP.Date_Packing AS Packing_Date,
    pp.Date_Closed as Close_Date , -1 as TAT_Closed  , -1 as TAT_Packing,
    --case when ee.Disposition <> '' then ee.Disposition else  a2.FCodeName  end
    '' as Test_Result,
    '' Final_Result,
    case when PP.fsta IN (29,39)  then PP.Error_VMI  ELSE '' end as NG_ErrorDesc,
    case when pp.fsta IN (29,39)  then PP.Error_VMI  ELSE '' end as NG_Reason,
    ee.fErrDesc as ErrDesc_VFIR, ee.fFailReason FailReason_VFIR,
    case when PP.id > 0 then 'Y' else 'N' end  Panel_Reapir,  a7.FCodeName Panel_Repair_type,
    f1.fCodeName as Delay_Type  , ff.fDesc Delay_Reason,
    ff.fRemark Delay_Category, ff.Cdate as Submit_Date, ff.Date_Dis as Recover_Date,
    RMA.fRequestREF as RequestRef, dd.Platform as Platform,
    PP.ReturnTimes as Return_times, rma.fSo as SO, RMA.fSt as Service_tag ,
    '' as Scrap , --ee.Scrap,
    PP.fResCode , PP.fsta, PP.fResult_VMI as fVIMRes,PP.fResult,
    PP.LastID, RR.Date_Closed as Last_Closed_Date, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote,
    RMA_RR.fSt as  Last_ServiceTAG,   RMA_RR.fRequestREF as  Last_RequestREF,
    '' DelayCategory, '' Delaytype, RMA_RR.fReturnTo as Last_Returnto, PP.date_closed as Date_Bad_Post,
    RMA.Returnto_Last
    --case when RR.Date_Closed <> '' then DATEDIFF(DAY, RR.Date_Closed, RMA.fRptDate) else 0 end AS Days_RR
    from ods_ODM_RMA_ITEM PP
    left outer join ods_cj_rmaitem RMA on RMA.Id = PP.OLDSYS_ID
    inner join ods_wx_ReturnTo_Base bb on pp.Returnto = bb.ReturnTo AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')
    left outer join ods_Holidays cc on to_date(pp.Date_Closed::text,'YYYY-MM-dd') = to_date(cc.fsDate::text,'YYYY-MM-dd')
    left outer join ods_ODM_PN_TPanel dd on dd.fPN = PP.fpn and dd.SvrCode = PP.SvrCode
    --left outer join cj_code a1 on a1.FType = 56 and a1.FCode = dd.fmSta
    left outer join ods_Screen_Disposition ee on ee.fScreenID = PP.id
    left outer join ods_ODM_OnHold_Item ff on ff.id = pp.OnHoldID
    left outer join ods_Base_ODM_OnHold f1 on ff.OnHoldType = f1.fcode
    LEFT OUTER JOIN ods_cj_panel_code a2 ON a2.FType = 23 AND a2.FParent = '0' AND a2.FCode = pp.fResult_VMI
    left outer join ods_cj_panel_code a7 on a7.FType = 23 and a7.FParent = '1' and a7.FCode = PP.PanelRepairType
    left outer join ods_ODM_RMA_ITEM RR on RR.id = PP.LastID
    left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.OLDSYS_ID
    where PP.SvrCode in (205,218) and Fn_ODM_isClosedSta(PP.fsta)=1 and PP.fsta<>-10
    and RMA.fReturnTo in (select ReturnTo from ods_wx_ReturnTo_Base where  CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA' ))
    and pp.Date_Closed >= '2020-01-01'
    and PP.updatetime::date=(now() - interval '1 day')::date) as tmp_panel;
    /*************************************************更新scrap 和 testresult************************************************/
update dwd_fact_Close_panel_OrderDetail aa
    set SCRAP = cj_KPI_DELL_IsScrap(aa.returnto, aa.SvrCode, aa.fsta, aa.frescode, aa.fVIMFunRes, aa.fRepairRes, bb.Scrap)
    ,   Test_Result = bb.Disposition
    from ods_Screen_Disposition bb
    where aa.SvrCode = 205 and  aa.Commodity = 'DT LCD'   and aa.Area in ('ARB', 'XM') -- and aa.SvrCode <> 70
    and bb.fScreenID = aa.id and bb.fsta in (19,29)
    and (  COALESCE(aa.SCRAP, '') <>  COALESCE(cj_KPI_DELL_IsScrap(aa.returnto, aa.SvrCode, aa.fsta, aa.frescode, aa.fVIMFunRes, aa.fRepairRes, bb.Scrap), '')
         or  COALESCE(aa.Test_Result, '') <>  COALESCE(bb.Disposition, ''))
    and aa.updatetime::date=(now() - interval '1 day')::date;

SELECT Dell_KPI_days('dwd_fact_close_panel_orderdetail');--更新其他

/*去重*/
DELETE FROM dwd_fact_close_panel_orderdetail
    WHERE ID_FLAG in (SELECT a1.ID_FLAG  FROM dwd_fact_close_panel_orderdetail a1,dwd_fact_close_panel_orderdetail a2
    where a1.Con=a2.Con and a1.updatetime<a2.updatetime);

/***************************************************Report_DELL_KPI_Item_WIP事实表****************************************************/
DROP TABLE IF EXISTS dwd_fact_Report_DELL_KPI_Item_WIP;
CREATE TABLE dwd_fact_Report_DELL_KPI_Item_WIP(
    AREA varchar(30) ,
	SvrCode int ,
	id int ,
	fym varchar(10) ,
	fqm int ,
	fmm int ,
	fwm int ,
	RETURNTO_RMA varchar(30) ,
	RETURNTO varchar(30) ,
	Commodity varchar(50) ,
	Control varchar(100) ,
	RMA varchar(100) ,
	PN varchar(30) ,
	PPID varchar(100) ,
	SN varchar(100) ,
	RMA_Status varchar(50) ,
	RMA_处理类型 varchar(50) ,
	Workstation varchar(50) ,
	Next_Workstation varchar(50) ,
	Model varchar(50) ,
	"Model Status" varchar(50) ,
	Date_SafeLunch timestamp ,
	"RMA issue_Date" timestamp ,
	Receive_Date timestamp ,
	Start_Date timestamp ,
	AgingDays int ,
	Aging varchar(50) ,
	"Over due" varchar(50) ,
	Test_Result varchar(50) ,
	Has_Repair varchar(30) ,
	"VMI Des." varchar(200) ,
	Panel_Reapir varchar(50) ,
	"Panel_Repair type" varchar(50) ,
	NG_Reason varchar(100) ,
	故障原因 varchar(100) ,
	"Is_On hold" varchar(10) ,
	Delay_Type varchar(50) ,
	Delay_Reason varchar(100) ,
	Delay_Category varchar(100) ,
	Submit_Date timestamp ,
	Recover_Date timestamp ,
	RequestRef varchar(50) ,
	Platform varchar(50) ,
	"Return times" int ,
	SO varchar(50) ,
	"Service tag #" varchar(50) ,
	TRPSN varchar(50) ,
	fResCode int ,
	fsta int ,
	fVIMFunRes int ,
	fRepairRes int ,
	RmaSta int ,
	RMAResCode int ,
	cDate timestamp ,
	LastID int ,
	Last_Closed_Date timestamp ,
	Days_RR int ,
	Last_Year_Code varchar(10) ,
	Last_Quarter_Code int ,
	Last_Month_Code int ,
	Last_Week_Code int ,
	Is90RR varchar(10) ,
	ImportantNote varchar(50) ,
	Last_ImportantNote varchar(50) ,
	DelayCategory varchar(100) ,
	DelayType varchar(100) ,
	ARB_YN varchar(1) ,
	排程日期 timestamp ,
	updatetime timestamp default now() - interval '1 day',
	ID_FLAG INT NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1)
)Distributed by (id);
set enable_nestloop =off;
/**/
INSERT INTO dwd_fact_Report_DELL_KPI_Item_WIP(area, svrcode, id, fym, fqm, fmm, fwm, returnto_rma,
                                              returnto, commodity, control, rma, pn, ppid, sn, rma_status, rma_处理类型, workstation,
                                              next_workstation, model, "Model Status", date_safelunch, "RMA issue_Date", receive_date,
                                              start_date, agingdays, aging, "Over due", test_result, has_repair, "VMI Des.", panel_reapir,
                                              "Panel_Repair type", ng_reason, 故障原因, "Is_On hold", delay_type, delay_reason, delay_category,
                                              submit_date, recover_date, requestref, platform, "Return times", so, "Service tag #", trpsn, frescode,
                                              fsta, fvimfunres, frepairres, rmasta, rmarescode,  lastid, last_closed_date,importantnote,
                                              last_importantnote, delaycategory, delaytype, 排程日期)
select
case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' End AS AREA,
bb.ServiceCode as SvrCode, aa.id, cc.fym fym , cc.fqm as fqm, cc.fmm fmm , cc.fwk fwm,
rma.fReturnTo as RETURNTO_RMA, aa.fReturnto as RETURNTO, bb.Commodity,
RMA.fCon as Control, RMA.frma as RMA, aa.fpn as PN, aa.fppid AS PPID,   aa.fsn as SN,
a5.FCodeName as RMA_Status, a6.FCodeName as RMA_处理类型,
a3.FCodeName Workstation,  a4.FCodeName as Next_Workstation, dd.Model,
case when aa.fRmaClass = 45    then
  case when dd.Sta_ARB in (2,6) THEN 'Safe Launch'  else 'NPI' end
else case when dd.fsta in (2,6) THEN 'Safe Launch'  else 'NPI' end
end as "Model Status", --a1.FCodeName
case when aa.fRmaClass = 45  then dd.Date_SafeLunch_ARB
else dd.Date_SafeLunch end as Date_SafeLunch,
RMA.Cdate as "RMA issue_Date",
RMA.fRptDate as Receive_Date, to_date(null::text,'YYYY-MM-dd HH24:MI:SS[.f...]')  as Start_Date,
-1 as AgingDays,
'' as Aging, --按区间分on way;0-5;6-10;11-19;20-29;30-60,>60
'' as "Over due",  --TAT vs TAT goal；正常，即将超期，超期三种
a2.FCodeName  as Test_Result,
''  Has_Repair,
'' "VMI Des.",
'' Panel_Reapir,  '' "Panel_Repair type",
aa.fErrDesc NG_Reason,   aa.fFailReasen 故障原因,
aa.fIsWait "Is_On hold", f1.fCodeName as Delay_Type  , ff.fWaitReason Delay_Reason,
ff.fRemark Delay_Category,  ff.Cdate as Submit_Date,  ff.fUnWaitDate as Recover_Date,
RMA.fRequestREF as RequestRef, dd.model as Platform,
aa.fReturnTimes "Return times", rma.fSo as SO, RMA.fSt as "Service tag #" ,
rma.fRecNumber as TRPSN , aa.fResCode , aa.fsta, aa.fVIMFunRes, aa.fRepairRes,
RMA.fSta as RmaSta, RMA.fResCode as RMAResCode,
aa.fLastID as LastID,  RR.fShipDate as Last_Closed_Date
, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote,
f1.DelayCategory as delayCategory,f1.DelayType as delaytype , aa.EasyPlanDate as 排程日期
from  ods_cj_oddItem aa
inner join ods_cj_rmaitem RMA on RMA.Id = aa.fItemid
inner join ods_wx_ReturnTo_Base bb on RMA.fReturnTo  = bb.ReturnTo AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')
left outer join ods_Holidays cc on RMA.fRptDate = to_date(cc.fsDate::text,'YYYY-MM-dd HH24:MI:SS[.f...]')
left outer join v_Screen_PN_Base dd on dd.PN = aa.fpn
left outer join ods_wx_Wait_master ff on ff.id = aa.fwaitid
left outer join ods_cj_Delay_Mast f1 on ff.fWaitType = f1.fcode
LEFT OUTER JOIN ods_cj_code a2 ON a2.FType = 22 AND a2.FParent = '7' AND a2.FCode = aa.fVIMFunRes
left outer join ods_cj_code a3 on a3.FType = 170 and a3.FCode =aa.fsta
left outer join ods_cj_code a4 on a4.FType = 170 and a4.FCode =aa.fNextSta
left outer join ods_cj_code a5 on a5.FType = 7 and a5.FCode = RMA.fSta
left outer join ods_cj_code a6 on a6.FType = 9 and a6.FCode = RMA.fResCode
left outer join ods_cj_oddItem RR on RR.id = aa.fLastID
left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.fItemid

where aa.fsta not IN (-3,19,29)
and bb.ServiceCode in (8, 16,17,18,  21,22,23, 25,26,27,28,32,36,39,43,44,45,87)
and RMA.fSta >=0
and aa.updatetime::date=(now() - interval '1 day')::date
union all
select case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' End AS AREA,
bb.ServiceCode as SvrCode, aa.id, cc.fym fym , cc.fqm as fqm,cc.fmm fmm, cc.fwk fwm,
rma.fReturnTo as RETURNTO_RMA, aa.fReturnto as RETURNTO, bb.Commodity,
RMA.fCon as Control, RMA.frma as RMA, aa.fpn as PN, aa.fppid AS PPID,   aa.fsn as SN,
a5.FCodeName as RMA_Status, a6.FCodeName as RMA_处理类型,
a3.FCodeName Workstation,  a4.FCodeName as Next_Workstation, dd.fmodel,
case when dd.fsta in (2,6) THEN 'Safe Launch'  else 'NPI' end as "Model Status", --a1.FCodeName
dd.fDate_Repair_Safe as Date_SafeLunch, RMA.Cdate as "RMA issue_Date",
RMA.fRptDate as Receive_Date, to_date(null::text,'YYYY-MM-dd HH24:MI:SS[.f...]')  as Start_Date,
-1 as AgingDays,
'' as Aging, --按区间分on way;0-5;6-10;11-19;20-29;30-60,>60
'' as "Over due",  --TAT vs TAT goal；正常，即将超期，超期三种
a2.FCodeName  as Test_Result,
''  Has_Repair,
'' "VMI Des.",
'' Panel_Reapir,  '' "Panel_Repair type",
aa.fErrDesc NG_Reason,   aa.fFailReasen 故障原因,
aa.fIsWait "Is_On hold", f1.fCodeName as Delay_Type  , ff.fWaitReason Delay_Reason,
ff.fRemark Delay_Category, ff.Cdate as Submit_Date, ff.fUnWaitDate as Recover_Date,
RMA.fRequestREF as RequestRef, dd.fmodel as Platform,
aa.fReturnTimes "Return times", rma.fSo as SO, RMA.fSt as "Service tag #" ,
rma.fRecNumber as TRPSN , aa.fResCode , aa.fsta, aa.fVIMFunRes, aa.fRepairRes,
RMA.fSta as RmaSta, RMA.fResCode as RMAResCode,
aa.fLastID as LastID, RR.fShipDate as Last_Closed_Date
, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote,f1.DelayCategory,
f1.DelayType,  aa.EasyPlanDate as 排程日期
from  ods_cj_oddItem aa
inner join ods_cj_rmaitem RMA on RMA.Id = aa.fItemid
inner join ods_wx_ReturnTo_Base bb on RMA.fReturnto = bb.ReturnTo AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')
left outer join ods_Holidays cc on RMA.fRptDate = to_date(cc.fsDate::text,'YYYY-MM-dd HH24:MI:SS[.f...]')
left outer join ods_cj_mem_model dd on dd.fpn = aa.fpn and aa.ODM = dd.ODM
left outer join ods_wx_Wait_master ff on ff.id = aa.fwaitid
left outer join ods_cj_Delay_Mast f1 on ff.fWaitType = f1.fcode
LEFT OUTER JOIN ods_cj_code a2 ON a2.FType = 22 AND a2.FParent = '7' AND a2.FCode = aa.fVIMFunRes
left outer join ods_cj_code a3 on a3.FType = 170 and a3.FCode =aa.fsta
left outer join ods_cj_code a4 on a4.FType = 170 and a4.FCode =aa.fNextSta
left outer join ods_cj_code a5 on a5.FType = 7 and a5.FCode = RMA.fSta
left outer join ods_cj_code a6 on a6.FType = 9 and a6.FCode = RMA.fResCode
left outer join ods_cj_oddItem RR on RR.id = aa.fLastID
left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.fItemid
where aa.fsta not IN (-3,19,29)
  and aa.fRmaClass in (15,24)
  and RMA.fSta > 0
  and aa.updatetime::date=(now() - interval '1 day')::date
union all
select case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' End AS AREA,
 bb.servicecode as SvrCode, aa.id, cc.fym fym , cc.fqm as fqm,cc.fmm fmm , cc.fwk fwm,
rma.fReturnTo as RETURNTO_RMA, aa.fReturnto as RETURNTO, bb.Commodity,
RMA.fCon as Control, RMA.frma as RMA, aa.fpn as PN, aa.fppid AS PPID,  rma.fSn as SN,
a5.FCodeName as RMA_Status, a6.FCodeName as RMA_处理类型,
a3.FCodeName Workstation,  a4.FCodeName as Next_Workstation, dd.Platform as Modal,
(case when fn_FPD_PN_fmsta(RMA.fReturnto,dd.fmSta,dd.fsta_ARB) in (2,6) THEN 'Safe Launch' else 'NPI' end) as "Model Status", --a1.FCodeName
fn_FPD_PN_ApproveDate(RMA.fReturnto,dd.ApproveDate,dd.Date_SafeLunch_ARB) as Date_SafeLunch, RMA.Cdate as "RMA issue_Date",
RMA.fRptDate as Receive_Date, to_timestamp(null::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Start_Date,
-1 as AgingDays,
'' as Aging, --按区间分on way;0-5;6-10;11-19;20-29;30-60,>60
'' as "Over due",  --TAT vs TAT goal；正常，即将超期，超期三种

a2.FCodeName  Test_Result,
aa.fIsRepairYN  Has_Repair,
'' "VMI Des.",
case when PP.id > 0 then 'Y' else 'N' end  Panel_Reapir,  a7.FCodeName "Panel_Repair type",
COALESCE(aa.fErrDesc, GG.FailureSymptom) as NG_Reason,   '' as 故障原因,
case when ff.fsta = 0 then 'Y' else COALESCE(pp.IsOnhold , 'N') end as  "Is_On hold",
COALESCE(f1.fCodeName, ii.fCodeName) as Delay_Type  ,
COALESCE(ff.fWaitReason, hh.fDesc) Delay_Reason,
COALESCE(ff.fRemark, hh.fRemark) Delay_Category, ff.Cdate as Submit_Date,ff.fUnWaitDate as Recover_Date,
RMA.fRequestREF as RequestRef, dd.Platform as Platform,
aa.fReTimes "Return times", rma.fSo as SO, RMA.fSt as "Service tag #" , rma.fRecNumber as TPRSN,
aa.fResCode , aa.fsta, aa.fVIMRes, aa.fRepairRes,RMA.fSta as RmaSta, RMA.fResCode as RMAResCode,
aa.fLastID as LastID,to_timestamp(RR.fDate_Close::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Last_Closed_Date
, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote
,COALESCE(f1.DelayCategory, ii.DelayCategory) as DelayCategory
,COALESCE(f1.DelayType, ii.DelayType) as DelayType,
aa.EasyPlanDate as 排程日期
from ods_wx_Item aa
inner join ods_cj_rmaitem RMA on RMA.Id = aa.fItemid
left outer join ods_ODM_RMA_ITEM PP on PP.id = aa.NewPanel_ID and PP.SvrCode = 202
inner join ods_wx_ReturnTo_Base bb on RMA.fReturnTo = bb.ReturnTo AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')
left outer join ods_Holidays cc on RMA.fRptDate = to_date(cc.fsDate::text,'YYYY-MM-dd HH24:MI:SS[.f...]')
left outer join ods_wx_Monitor dd on dd.fPN = aa.fpn
--left outer join cj_code a1 on a1.FType = 56 and a1.FCode = dd.fmSta
--left outer join wx_Disposition ee on ee.fwxID = aa.id
left outer join ods_wx_Wait_master ff on ff.id = aa.fwaitid
left outer join ods_cj_Delay_Mast f1 on ff.fWaitType = f1.fcode
LEFT OUTER JOIN ods_cj_code a2 ON a2.FType = 22 AND a2.FParent = '7' AND a2.FCode = aa.fVIMRes
left outer join ods_cj_panel_code a7 on a7.FType = 23 and a7.FParent = '1' and a7.FCode = PP.PanelRepairType
left outer join ods_cj_code a3 on a3.FType = 41 and a3.FCode =aa.fsta
left outer join ods_cj_code a4 on a4.FType = 41 and a4.FCode =aa.fNextSta
left outer join ods_cj_code a5 on a5.FType = 7 and a5.FCode = RMA.fSta
left outer join ods_cj_code a6 on a6.FType = 9 and a6.FCode = RMA.fResCode
left outer join ods_wx_Item RR on RR.id = aa.fLastID
left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.fItemid
left outer join ods_cj_Test_mast GG ON GG.id = PP.TestID_FA
left outer join ods_ODM_OnHold_Item hh on hh.id = PP.OnHoldID
left outer join ods_Base_ODM_OnHold ii on hh.OnHoldType = ii.fcode

where  aa.fsta not IN (-3,-5,34,39,49,59,69, 19,51,52,53)
and RMA.fSta  >=0 --not in (-3, 29, 59)
and RMA.fReturnTo in ('CJREPAIR',  'CJSPHL', 'ARBCJMON','CJINUSE')  --
and aa.updatetime::date=(now() - interval '1 day')::date
union all
select distinct case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' End AS AREA,
pp.SvrCode as SvrCode, PP.id, cc.fym fym ,cc.fqm as fqm, cc.fmm fmm , cc.fwk fwm,
rma.fReturnTo as RETURNTO_RMA, PP.Returnto as RETURNTO, bb.Commodity,
RMA.fCon as Control, RMA.frma as RMA, PP.fpn as PN, PP.MonitorPPID AS PPID, pp.PPID as SN, -- rma.fSn
a5.FCodeName as RMA_Status, a6.FCodeName as RMA_处理类型,
a3.FCodeName Workstation,  a4.FCodeName as Next_Workstation, dd.Platform as Modal,
case when dd.fsta in (6) THEN 'Safe Launch'  else 'NPI' end as "Model Status", --a1.FCodeName
dd.Date_SafeLunch as Date_SafeLunch,RMA.Cdate as "RMA issue_Date",
RMA.fRptDate as Receive_Date, to_date(null::text,'YYYY-MM-dd HH24:MI:SS[.f...]') as Start_Date,
-1 as AgingDays,
'' as Aging, --按区间分on way;0-5;6-10;11-19;20-29;30-60,>60
'' as "Over due",  --TAT vs TAT goal；正常，即将超期，超期三种

a2.FCodeName  Test_Result,
PP.PanelRepairYN as Has_Repair,
pp.Error_VMI "VMI Des.",
case when PP.id > 0 then 'Y' else 'N' end  Panel_Reapir,  a7.FCodeName "Panel_Repair type",
'' as NG_Reason,   '' as 故障原因,
pp.IsOnhold  as  "Is_On hold",--case when ff.fsta = 0 then 'Y' else 'N' end
f1.fCodeName as Delay_Type  ,
ff.fDesc Delay_Reason,
ff.fRemark Delay_Category,ff.Cdate as Submit_Date, ff.Date_Dis as Recover_Date,
RMA.fRequestREF as RequestRef, dd.Platform as Platform,
pp.ReturnTimes "Return times", rma.fSo as SO, RMA.fSt as "Service tag #" , rma.fRecNumber as TPRSN,
PP.fResCode , PP.fsta,PP.fResult_VMI as fVIMRes,pp.fRescode as fRepairRes,RMA.fSta as RmaSta, RMA.fResCode as RMAResCode,
PP.LastID as LastID, RR.Date_Closed as Last_Closed_Date
, RMA.ImportantNote, RMA_RR.ImportantNote as Last_ImportantNote,
f1.DelayCategory,f1.DelayType, pp.EasyPlanDate as 排程日期
from ods_ODM_RMA_ITEM PP
inner join ods_cj_rmaitem RMA on RMA.Id = PP.OLDSYS_ID
inner join ods_wx_ReturnTo_Base bb on RMA.fReturnTo = bb.ReturnTo AND CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA')
left outer join ods_Holidays cc on RMA.fRptDate = to_date(cc.fsDate::text,'YYYY-MM-dd HH24:MI:SS[.f...]')
left outer join  ods_ODM_PN_TPanel dd on dd.fPN = PP.fpn  and dd.SvrCode = PP.SvrCode
--left outer join cj_code a1 on a1.FType = 56 and a1.FCode = dd.fmSta
--left outer join wx_Disposition ee on ee.fwxID = aa.id
left outer join ods_ODM_OnHold_Item ff on ff.id = pp.OnHoldID
left outer join ods_Base_ODM_OnHold f1 on ff.OnHoldType = f1.fcode
LEFT OUTER JOIN ods_cj_panel_code a2 ON a2.FType = 23 AND a2.FParent = '0' AND a2.FCode = PP.fResult_VMI
left outer join ods_cj_panel_code a7 on a7.FType = 23 and a7.FParent = '1' and a7.FCode = PP.PanelRepairType
left outer join ods_cj_panel_code a3 on a3.FType = 7 and a3.FCode =PP.fsta
left outer join ods_cj_panel_code a4 on a4.FType = 7 and a4.FCode =PP.fNextSta
left outer join ods_cj_code a5 on a5.FType = 7 and a5.FCode = RMA.fSta
left outer join ods_cj_code a6 on a6.FType = 9 and a6.FCode = RMA.fResCode
left outer join ods_odm_rma_item RR on RR.id = PP.LastID
left outer join ods_cj_rmaitem RMA_RR on RMA_RR.Id = RR.OLDSYS_ID
where  PP.SvrCode in (205,218)
 and Fn_ODM_isClosedSta(PP.fsta)=0
 and RMA.fSta >=0 --not in (-3, 29, 59)
 and PP.updatetime::date=(now() - interval '1 day')::date;

INSERT INTO dwd_fact_Report_DELL_KPI_Item_WIP(area, svrcode, id, fym, fmm, fwm, returnto_rma,
                                              returnto, commodity, control, rma, pn, ppid, sn,
                                              rma_status, rma_处理类型,"Return times",requestref,
                                              so,"Service tag #",trpsn,importantnote, "RMA issue_Date",
                                              rmasta, rmarescode)
select
case when bb.CustomerCode = 'DELL ARB' then 'ARB' else 'XM' End AS AREA,
bb.ServiceCode as SvrCode , aa.Id, null as fym, null fmm, null as fwm, aa.fReturnTo as Returnto_RMA,
'' as Returnto, bb.Commodity, aa.fCon Control, aa.frma as RMA, aa.fPn PN,  fPPid as PPID, aa.fSn AS SN, 'OnWay' as Rma_Status,
'' as RMA_处理类型,   aa.ftimes as "Return times", aa.fRequestREF as RequestRef,
 aa.fso as SO, aa.fSt as "Service tag #", fRecNumber as TRPSN, aa.ImportantNote,
aa.Cdate  as "RMA issue_Date", aa.fSta as RmaSta, aa.fResCode as RMAResCode
from ods_cj_rmaitem aa
inner join ods_wx_ReturnTo_Base bb on aa.fReturnTo = bb.ReturnTo
  AND bb.CustomerCode in ('DELL', 'DELL ARB', 'DELL_PNA' )
--left outer join cj_Holidays cc on CONVERT(date, cc.fdate) = CONVERT(date, aa.cdate)
where aa.fSta = 0
and  bb.Commodity in (SELECT  commodity FROM ods_commodity_color_aging_model where Area in ('XM', 'ARB'))
and aa.updatetime::date=(now() - interval '1 day')::date;

SELECT COUNT(*)
  FROM dwd_fact_Report_DELL_KPI_Item_WIP
  where Receive_Date is not null;

update dwd_fact_Report_DELL_KPI_Item_WIP
set Start_date = cj_Get_StartDate(SvrCode, Receive_Date, Date_safelunch, null )
    where Area in ('XM','ARB')
      and updatetime::date=(now() - interval '1 day')::date;


DROP TABLE IF EXISTS temp_AgingDays;
CREATE TEMPORARY TABLE temp_AgingDays(id INT,AgingDays INT);
INSERT INTO temp_AgingDays(ID, AgingDays)
SELECT ID,AgingDays FROM
(
SELECT dfRDKIW.ID AS ID,COUNT(ho.id) AS AgingDays
FROM ods_holidays ho,dwd_fact_Report_DELL_KPI_Item_WIP dfRDKIW
WHERE (
  dfRDKIW.updatetime::date=(now() - interval '1 day')::date
  and ho.fdate between dfRDKIW.start_date and now()
  and ho.fishday = FALSE
  and dfRDKIW.Area in ('ARB', 'XM')
  and dfRDKIW.updatetime::date=(now() - interval '1 day')::date
    )
GROUP BY dfRDKIW.ID
) AS TMP;
update dwd_fact_Report_DELL_KPI_Item_WIP dfRDKIW set (ID,AgingDays) =(
		SELECT I.id,AgingDays FROM temp_AgingDays I WHERE dfRDKIW.ID = I.ID)
		where EXISTS(SELECT temp_AgingDays.id FROM temp_AgingDays where temp_AgingDays.id=dfRDKIW.id)
           and updatetime::date=(now() - interval '1 day')::date;
select id,AgingDays,AREA,start_date from dwd_fact_Report_DELL_KPI_Item_WIP;


update dwd_fact_Report_DELL_KPI_Item_WIP
set Aging = Fn_AgingDayPer_KPI(AgingDays)
where Area in ('XM','ARB')
  and updatetime::date=(now() - interval '1 day')::date;

update dwd_fact_Report_DELL_KPI_Item_WIP
set (Last_Year_Code,Last_Quarter_Code,Last_Month_Code,Last_Week_Code,Days_RR,Is90RR)
    = (bb.fym, bb.fqm,bb.fmm,bb.fwk,date_part('day', aa.Receive_Date::timestamp - aa.Last_Closed_Date::timestamp),
       case when date_part('day', aa.Receive_Date-aa.Last_Closed_Date ) <=90 then 'Y' else 'N' end)
from dwd_fact_Report_DELL_KPI_Item_WIP aa
inner join ods_holidays bb on aa.Last_Closed_Date::date = bb.fdate::date
where aa.Area in ('XM','ARB')  and  aa.LastID > 0 and aa.Last_Closed_Date is not null
and aa.updatetime::date=(now() - interval '1 day')::date;
/*去重*/
DELETE FROM dwd_fact_Report_DELL_KPI_Item_WIP
    WHERE ID_FLAG in (SELECT a1.ID_FLAG  FROM dwd_fact_Report_DELL_KPI_Item_WIP a1,dwd_fact_Report_DELL_KPI_Item_WIP a2
    where a1.Control=a2.Control and a1.updatetime<a2.updatetime);

select date_part('day','2021-04-28 00:00:00.000000'::timestamp-'2021-04-10 00:00:00.000000'::timestamp)

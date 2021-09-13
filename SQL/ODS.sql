CREATE DATABASE CJ_tprdb;

/*********************************************************包装**********************************************************/
DROP TABLE IF EXISTS ods_cj_rmaitem_PackRemark;
CREATE TABLE ods_cj_rmaitem_PackRemark(
    fitemid int NOT NULL ,
    fRemark varchar(100),
    cDate timestamp,
    cUser varchar(10),
    Unit_condition varchar(50),
    Packing_condition varchar(50),
    updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fitemid);

/*触发器*/
create or replace function update_timestamp_cj_rmaitem_PackRemark() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_rmaitem_PackRemark on ods_cj_rmaitem_PackRemark;
create trigger auto_updatetime_cj_rmaitem_PackRemark
	before update on ods_cj_rmaitem_PackRemark
	for each row execute procedure update_timestamp_cj_rmaitem_PackRemark();
/****************************************************DELL产品RMA主表*****************************************************/
DROP TABLE IF EXISTS ods_cj_Rmaitem;
CREATE TABLE ods_cj_Rmaitem(
    Id int primary key NOT NULL,
	fRecNo varchar(20),
	fChkNo varchar(20),
	fSRNo varchar(20),
	fSeq int,
	frma varchar(100) ,
	fCon varchar(100) ,
	fSo varchar(50),
	fReturnTo varchar(20),
	fSt varchar(20),
	fPn varchar(20),
	fSn varchar(100),
	fRPN varchar(15),
	fDesc varchar(100),
	fPrice decimal(18,4),
	fFReason varchar(200),
	fdays int,
	ftimes int,
	fRemark varchar(100),
	fSort int,
	fSta int,
	fEditSta int ,
	fPNchkSta int ,
	fSendTo varchar(20),
	fPPid varchar(100),
	RTime int ,
	Cdate timestamp,
	FOEMSN varchar(50),
	fResCode int ,
	fOldPPID varchar(50),
	foldsn varchar(50),
	fOldPn varchar(10),
	fPhysic varchar(100),
	fHawb varchar(20),
	fHawbSn varchar(30),
	fIntegrity varchar(100),
	fStaRes varchar(100),
	RAID int,
	ORID int,
	SRID int,
	DEID int,
	CRID int,
	SCID int,
	SHID int,
	ACID int,
	GDID int,
	RTID int,
	RTSR int,
	RTSC int,
	OEMT int,
	fDESta int,
	fOEMRma varchar(25),
	fEmailSta int,
	fRecNumber varchar(30),
	fRequestREF varchar(20),
	fRmaClass int,
	FLOC varchar(30),
	fCkResCode int ,
	ftestSta int,
	fSRBox int,
	fCloseDate timestamp,
	fAgentDate varchar(20),
	fRptDate timestamp,
	fSRHawb varchar(30),
	fDept varchar(10),
	fScarpReason varchar(100),
	fScarpConf varchar(20),
	fsolID int,
	fGSRType int,
	fScrapFile varchar(100),
	fTDMP_DENo varchar(30),
	fbrand varchar(30),
	fLastSta int,
	fFUJITSU_NO varchar(20),
	fWarranty int,
	fProDate varchar(20),
	fIsRmaClassChange char(1),
	fDate_RmaclassChange timestamp,
	fUser_RmaClassChange varchar(10),
	fOemRma_RmaClassChange varchar(30),
	fver varchar(5),
	fRecAssigner varchar(10),
	fDelayID int,
	fDelayType int,
	fDelayReason varchar(50),
	fDelayPostSta int,
	So_closedate varchar(30),
	fServiceCode int,
	fDate_Judge timestamp,
	fDell_ID int,
	fdate_OemRma timestamp,
	fUser_OemRma varchar(10),
	fDate_Scrap timestamp,
	sUser_Scrap varchar(10),
	fDate_SR timestamp,
	fUser_SR varchar(10),
	fDSPAWB_FromDell varchar(50),
	fRmaAssignDate timestamp,
	ImportantNote varchar(30),
	fNonServerPn varchar(6),
	fNonServerPPID varchar(30),
	RODJudgeYN varchar(1),
	RODJudgeDate timestamp,
	RODJudgeUser varchar(10),
	RODJudgeLastSendto varchar(20),
	RODJudgeRequestDate timestamp,
	ETA timestamp,
	Date_ReceiveAssign timestamp,
	User_ReceiveAssign varchar(10),
	Label_DSP varchar(30),
	ARBFlag int,
	LPTA varchar(50),
	COO varchar(20),
	PN_Ship varchar(10),
	RCPID int,
	RepairToPN varchar(10),
	ResultCode_Rec int,
	Date_SRCheck timestamp,
	RNU varchar(5),
	ReturnTo_First varchar(30),
	Date_Received_First timestamp,
	SR_TYPE int,
	ReturnTo_Last varchar(30),
	Depot varchar(30),
	Date_Received_SH timestamp,
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (Id);


update ods_cj_rmaitem
set updatetime=( now() - interval '1 day')
where updatetime::date=current_timestamp::date;

/*触发器*/
create or replace function update_timestamp_cj_rmaitem() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_rmaitem on ods_cj_Rmaitem;
create trigger auto_updatetime_cj_rmaitem
	before update on ods_cj_Rmaitem
	for each row execute procedure update_timestamp_cj_rmaitem();


UPDATE ods_cj_rmaitem
    SET Cdate = NULL
WHERE Cdate = '1900-09-09';
UPDATE ods_cj_rmaitem
    SET Date_Received_First = NULL
WHERE Date_Received_First = '1900-09-09';
UPDATE ods_cj_rmaitem
    SET Date_Received_SH = NULL
WHERE Date_Received_SH = '1900-09-09';

/**************************************************判断是否变更Return表***************************************************/
DROP TABLE IF EXISTS ods_cj_RmaChange_Returnto;
CREATE TABLE ods_cj_RmaChange_Returnto(
    id int primary key NOT NULL,
	RmaItemID int,
	fcon varchar(50),
	fPPID varchar(50),
	Returnto_From varchar(30),
	Returnto_To varchar(30),
	RequestRma varchar(50),
	Date_Received timestamp,
	Date_Closed timestamp,
	cDate timestamp,
	cUser varchar(10),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (id);

update ods_cj_RmaChange_Returnto
set updatetime=(now() - interval '1 day')
where updatetime::date=now()::date;

/*触发器*/
create or replace function update_timestamp_cj_RmaChange_Returnto() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_RmaChange_Returnto on ods_cj_RmaChange_Returnto;
create trigger auto_updatetime_cj_RmaChange_Returnto
	before update on ods_cj_RmaChange_Returnto
	for each row execute procedure update_timestamp_cj_RmaChange_Returnto();
/********************************************************RMA表*********************************************************/
DROP TABLE IF EXISTS ods_cj_Rma;
CREATE TABLE ods_cj_Rma(
    Id int PRIMARY KEY NOT NULL ,
	fRecNo varchar(20) ,
	fCodetype varchar(20) ,
	fDeptcode varchar(20) ,
	fDate varchar(50) ,
	fIMPORTANT varchar(30) ,
	fRma varchar(50) ,
	fCusName varchar(100) ,
	fCusAdd varchar(200) ,
	fCusTel varchar(100) ,
	fCusFax varchar(50) ,
	fContact varchar(100) ,
	fRequestREF varchar(30) ,
	fsta int ,
	fChecker varchar(20) ,
	fMem varchar(255) ,
	fCreate_date timestamp ,
	fCreate_user varchar(20) ,
	fEmail varchar(200) ,
	fReqClass int ,
	fInputEmailID int ,
	fOutEmailID int ,
	fOEM varchar(20) ,
	fCloseDate varchar(20) ,
	fSRHawb varchar(30) ,
	fremark text ,
	fisNeedbrand char(1),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (Id);
SELECT COUNT(*) FROM ods_cj_Rma;
/*触发器*/
create or replace function update_timestamp_cj_Rma() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Rma on ods_cj_Rma;
create trigger auto_updatetime_cj_Rma
	before update on ods_cj_Rma
	for each row execute procedure update_timestamp_cj_Rma();
/***************************************************wx_ReturnTo_Base***************************************************/
DROP TABLE IF EXISTS ods_wx_ReturnTo_Base;
CREATE TABLE ods_wx_ReturnTo_Base(
    id int ,
	Customer varchar(50) ,
	ReturnTo varchar(30) PRIMARY KEY,
	ServiceCode int ,
	Commodity varchar(50) ,
	ServiceCategory varchar(50) ,
	RmaPrefix varchar(20) ,
	Wip varchar(20) ,
	ServiceResult varchar(50) ,
	Remark varchar(100) ,
	BusinessCode varchar(10) ,
	SubCode int ,
	CustomerCode varchar(30) ,
	CDate timestamp ,
	CName varchar(10) ,
	IW_TORepair char(1) ,
	RepairTips varchar(100) ,
	IsINUSE char(1) ,
	IsCJRepair char(1) ,
	IsNeedCall_Log varchar(1) ,
	HasTouch char(1) ,
	IsCJPlanning char(1) ,
	PlanningMethod varchar(30) ,
	IsClosed char(1) ,
	NPITagQty int ,
	ProjectID int ,
	Priority int ,
	SettlementCode varchar(10) ,
	Rec_Type varchar(10) ,
	TPRSN_Pre varchar(20) ,
	PlanningCustomer varchar(30) ,
	PlanningArea varchar(10) ,
	PlanningUser varchar(40) ,
	PlanningRepairBusiness varchar(50) ,
	PlanningRepairDep varchar(50) ,
	PlanningDuty varchar(100) ,
	PlanningMethodCode varchar(2) ,
	PlanningMethodRemark varchar(100) ,
	PlanningIsShow varchar(1) ,
	PlanningRemark varchar(100) ,
	PlanningWipQty int ,
	PlanningOnwayQty int ,
	PlanningRcvQty int ,
	PlanningUseQty int ,
	PlanningModifiedUser varchar(10) ,
	PlanningModifiedDate timestamp ,
	NPITATGoal int ,
	SafeLaunchTATGoal int ,
	BeginDate timestamp ,
	EndDate timestamp ,
	AwpSource int ,
	SalPanelCalYN varchar(1) ,
	SafeLaunchTATType varchar(10) ,
	ShipFre varchar(10) ,
	IsSchedule varchar(1) ,
	PM varchar(10) ,
	RRGoal int ,
	良率GOAL int ,
	TATGoal int ,
	TATGoal日期类别 varchar(4) ,
	OnwayTAT int ,
	OnwayTAT日期类别 varchar(4) ,
	维修TAT int ,
	维修TAT日期类别 varchar(4) ,
	系统内TAT起点 timestamp ,
	系统内TAT终点 timestamp ,
	RMAIssue int ,
	提货 int ,
	到港 int ,
	清关开始 int ,
	清关结束 int ,
	CJ签收 int ,
	接收 int ,
	关单 int ,
	发货 int ,
	客户签收 int ,
	包材类别 varchar(20) ,
	包材渠道 varchar(20) ,
	备注 varchar(200) ,
	MaterialOwner varchar(20),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (ReturnTo);
/*触发器*/
create or replace function update_timestamp_wx_ReturnTo_Base() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_wx_ReturnTo_Base on ods_wx_ReturnTo_Base;
create trigger auto_updatetime_wx_ReturnTo_Base
	before update on ods_wx_ReturnTo_Base
	for each row execute procedure update_timestamp_wx_ReturnTo_Base();

update ods_wx_ReturnTo_Base
set updatetime=( now() - interval '1 day')
where updatetime::date=current_timestamp::date;

/*****************************************************Base_Service*****************************************************/
DROP TABLE IF EXISTS ods_Base_Service;
CREATE TABLE ods_Base_Service(
    id int NOT NULL ,
	Code varchar(10) ,
	Name varchar(200) ,
	Remark varchar(200) ,
	ServiceCode int PRIMARY KEY NOT NULL ,
	ServiceParent int ,
	ServiceCodeName varchar(100) ,
	IsActive boolean ,
	CDate timestamp ,
	CUser varchar(10) ,
	BasePNView varchar(30) ,
	IsNeedVer char(1) ,
	IsMSRCheck char(1) ,
	IsHouseMa char(1) ,
	Dep varchar(30) ,
	IsDellBusiness boolean ,
	fDepCode int ,
	TestType varchar(10) ,
	HighValuePrice float ,
	IT varchar(20) ,
	WarehouseLevel int ,
	ODMItem_Table varchar(50) ,
	SettlementDepartment varchar(50),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (ServiceCode);

/*触发器*/
create or replace function update_timestamp_Base_Service() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_Base_Service on ods_Base_Service;
create trigger auto_updatetime_Base_Service
	before update on ods_Base_Service
	for each row execute procedure update_timestamp_Base_Service();
/********************************************************Holidays******************************************************/
DROP TABLE IF EXISTS ods_Holidays;
CREATE TABLE ods_Holidays(
    id int  NOT NULL,
	fyy varchar(10) ,
	fym varchar(10) ,
	fqm int ,
	fmm int ,
	fwk int ,
	fsDate varchar(20) primary key ,
	fisHDay boolean,
	fDay int ,
	fDesc varchar(30) ,
	fdate timestamp,
    updatetime timestamp default (now() - interval '1 day'))
Distributed replicated;
/*触发器*/
create or replace function update_timestamp_Holidays() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_Holidays on ods_Holidays;
create trigger auto_updatetime_Holidays
	before update on ods_Holidays
	for each row execute procedure update_timestamp_Holidays();
/********************************************************Vendor********************************************************/
DROP TABLE IF EXISTS ods_cj_vendor;
CREATE TABLE ods_cj_vendor
(
    Fid                int,
    FVenCode           varchar(20),
    FVenName           varchar(100),
    FVenTel            varchar(50),
    FVenFax            varchar(50),
    Ftype              int,
    FReturnTo          varchar(20) PRIMARY KEY NOT NULL ,
    Commodity          varchar(50),
    ARM_Request_type   varchar(20),
    IsBlockRma         varchar(2),
    Warranty_Period    varchar(20),
    OEM_issue_period   varchar(20),
    RMA_CONTACT        varchar(50),
    RMA_Format         varchar(20),
    Format_Remark      varchar(50),
    Request_Frequency  varchar(50),
    Request_RMA_Email  varchar(200),
    Issue_RMA_Email    varchar(50),
    Country            varchar(20),
    Shipment_frequency varchar(50),
    Shipping_Add       varchar(500),
    Receiver_Name      varchar(100),
    Receiver_Tel       varchar(100),
    Freight            varchar(50),
    CCC_Vendor         varchar(20),
    FORWARDER          varchar(500),
    Comment            varchar(50),
    OEM_Planner        varchar(20),
    Oversea_Local      varchar(20),
    Dell_Support       varchar(20),
    IsVerifySn         char(1),
    fBrand             varchar(20),
    fWarranty_Month    int,
    ODM                varchar(30),
    Is_CJ              varchar(1),
    RL_LabYN           varchar(1),
    Is_NeedPhoto       varchar(1),
    ReturnType         varchar(20),
    updatetime timestamp default (now() - interval '1 day')
)
Distributed by (FReturnTo);
/*触发器*/
create or replace function update_timestamp_cj_vendor() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_vendor on ods_cj_vendor;
create trigger auto_updatetime_cj_vendor
	before update on ods_cj_vendor
	for each row execute procedure update_timestamp_cj_vendor();
/*******************************************************CJ_Code********************************************************/
DROP TABLE IF EXISTS ods_cj_Code;
CREATE TABLE ods_cj_Code(
    id int  primary key ,
	FType int ,
	FTypeName varchar(50) ,
	FCode int ,
	FCodeName varchar(100) ,
	FParent varchar(20),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (id);
/*触发器*/
create or replace function update_timestamp_cj_Code() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Code on ods_cj_Code;
create trigger auto_updatetime_cj_Code
	before update on ods_cj_Code
	for each row execute procedure update_timestamp_cj_Code();
/*******************************************************CJ_Odditem********************************************************/
DROP TABLE IF EXISTS ods_cj_Odditem;
CREATE TABLE ods_cj_Odditem(
    id int ,
    fsta int ,
    fIsWait char(1),
    fRmaClass int ,
    fItemid int primary key ,
    fDate_HandOver_in timestamp ,
    fDate_HandOver_out timestamp ,
    fDate_HandOver_Rec timestamp ,
    fUser_HandOver_Rec varchar(10) ,
    fDate_PostRepair timestamp ,
    fDate_Receive timestamp,
    fReceiver varchar(10) ,
    fRecVer varchar(10) ,
    fVer varchar(10) ,
    fVerYN char(1) ,
    fRepairYN int ,
    fNextSta int ,
    fisErr int ,
    fResCode int ,
    fReturnTimes int ,
    fLastID int ,
    fVIMFunRes int ,
    fErrDesc varchar(50) ,
    fErrDesc_En varchar(50) ,
    fFailReasen varchar(50) ,
    fFailReasen_En varchar(50) ,
    fisOBA int ,
    fOBA_QTY int ,
    fOBARemark varchar(50) ,
    fOBA_Times int ,
    fOBA_No varchar(50) ,
    frmaid int ,
    fTestStep int ,
    fFailureType int ,
    fWarranty int ,
    fProDate timestamp,
    fEcrRemark varchar(50) ,
    fEcrDate timestamp ,
    fHasDesk char(1) ,
    fRepairRemark varchar(50) ,
    fRepairRes int ,
    fIsReplacePN char(1) ,
    fLastShipDate varchar(50) ,
    fppid varchar(100) ,
    fsn varchar(100) ,
    fReturnto varchar(50) ,
    fShipDate timestamp,
    fDate_FinishTest timestamp ,
    fpn varchar(50) ,
    fDate_Screen timestamp ,
    fUser_Screen varchar(10) ,
    fPrc varchar(50) ,
    fECNYN char(1) ,
    fEcnDate timestamp ,
    fEcnNo varchar(50) ,
    fECRLable char(1) ,
    fShipMent_PN varchar(50) ,
    fShipMent_PPID varchar(50) ,
    FW varchar(50) ,
    SevericeTag varchar(50) ,
    fWaitID int ,
    EServerCode varchar(50) ,
    ELabKCode char(1) ,
    fTprsn varchar(50) ,
    floc varchar(50) ,
    flog text,
    fDate_Repair timestamp ,
    fEngineer_Repair varchar(10) ,
    fEngineer_Repair1 varchar(10) ,
    fDate_NextC timestamp ,
    fDate_Pack timestamp ,
    fUser_Pack varchar(10) ,
    fisAssign char(1) ,
    fUser_Assign varchar(10) ,
    fDate_Assign timestamp ,
    fEngineer_Assign varchar(10) ,
    fDate_BadPost timestamp ,
    fUser_BadPost varchar(10) ,
    fHouseID int ,
    fHouseID_Post int ,
    fHouseSta int ,
    fDate_InHousePost timestamp ,
    fUser_InHousePost varchar(10) ,
    fBGADateCode varchar(10) ,
    fDate_OutHouse timestamp ,
    fUser_OutHouse varchar(10) ,
    fIsBGAError char(1) ,
    fMemVimType int ,
    fMemRepairType int ,
    fBGAPn varchar(50) ,
    fOBAReCheck_Date timestamp ,
    fOBAReCheck_Rno varchar(50) ,
    fOBARecheck_Poster varchar(10) ,
    foem varchar(50) ,
    fDate_Burn_Begin timestamp ,
    fUser_Burn_Begin varchar(10) ,
    fDate_Burn_End timestamp ,
    fUser_Burn_End varchar(10) ,
    fisUnRepaired char(1) ,
    ODM varchar(50) ,
    BGAFlg char(1) ,
    QACheckUser varchar(10) ,
    QACheckDate timestamp ,
    QACheckResult char(1) ,
    BIOS varchar(60) ,
    MAC varchar(200) ,
    PSn varchar(50) ,
    RecBIOS varchar(60) ,
    RepairAction varchar(50) ,
    PSn1 varchar(50) ,
    PSn2 varchar(50) ,
    PSn3 varchar(50) ,
    Cardbus_type varchar(10) ,
    VGA varchar(10) ,
    PNo_Runin varchar(50) ,
    QAID int ,
    SN1 varchar(50) ,
    SVRMAC varchar(160) ,
    fVimBistResult varchar(50) ,
    VER22P varchar(10) ,
    ETA timestamp ,
    OEM_TestID int ,
    PN_RMA varchar(50) ,
    HopeDate timestamp ,
    EasyPlanDate timestamp ,
    EasyPlanNO varchar(10) ,
    BinItem_fNo varchar(50) ,
    SRBoxID int ,
    BinItem_fsta int ,
    Pre_PlanDate timestamp ,
    Pre_PlanUser varchar(50) ,
    PackingGroup_NO int ,
    RDPK varchar(50) ,
    Date_RDPK timestamp ,
    TestID_QT1 int ,
    Result_QT1 int ,
    ErrCode_QT1 varchar(50) ,
    FirstNPI boolean ,
    TotTAT int ,
    NowTAT int ,
    Date_Request_PackingMat timestamp ,
    User_Request_PackingMat varchar(50) ,
    fDate_OutFromHouse_Plan timestamp ,
    fUser_OutFromHouse_Plan varchar(10),
    updatetime timestamp default (now() - interval '1 day')

)
Distributed by (fItemid);
/*触发器*/
create or replace function update_timestamp_cj_Odditem() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Odditem on ods_cj_Odditem;
create trigger auto_updatetime_cj_Odditem
	before update on ods_cj_Odditem
	for each row execute procedure update_timestamp_cj_Odditem();
/*******************************************************cj_odd_PN********************************************************/
DROP TABLE IF EXISTS ods_cj_odd_PN;
CREATE TABLE ods_cj_odd_PN(
    id int ,
	fRmaClass int ,
	fpn varchar(6) PRIMARY KEY ,
	fOem varchar(20) ,
	fVer varchar(3) ,
	fFWVer varchar(10) ,
	fProgram varchar(30) ,
	fDesc varchar(50) ,
	fCDR char(1) ,
	fDVDR char(1) ,
	fCDW char(1) ,
	fDVDW char(1) ,
	fModel varchar(20) ,
	fremark varchar(50) ,
	fwarranty int ,
	fuser varchar(10) ,
	fUpdate timestamp ,
	fsta int ,
	CartonPN varchar(50) ,
	PackPN varchar(50) ,
	fCutin timestamp ,
	fPhasOut timestamp ,
	fisClosed char(1) ,
	FixtureNO varchar(20) ,
	fScrapVer varchar(10) ,
	fxr varchar(1) ,
	fDate_Repair_Safe timestamp ,
	fUser_REpair_Safe varchar(10) ,
	fUpgradePN varchar(10) ,
	fCommodity varchar(30) ,
	fjk varchar(20) ,
	fClass varchar(20) ,
	fPrcClass varchar(30) ,
	fShipMentPN varchar(5) ,
	cDate timestamp ,
	PACKPN_DELL varchar(30),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);


/*触发器*/
create or replace function update_timestamp_cj_odd_PN() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT (now() - interval '1 day') INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_odd_PN on ods_cj_odd_PN;
create trigger auto_updatetime_cj_odd_PN
	before update on ods_cj_odd_PN
	for each row execute procedure update_timestamp_cj_odd_PN();
/*******************************************************cj_HDD_PN********************************************************/
DROP TABLE IF EXISTS ods_cj_HDD_PN;
CREATE TABLE ods_cj_HDD_PN(
    fpn varchar(6) primary key ,
	fsta int ,
	fRmaClass int ,
	isScreenYN char(1) ,
	fshipPN varchar(6) ,
	fVer varchar(5) ,
	fmodel varchar(30) ,
	fdesc varchar(100) ,
	fClass varchar(20) ,
	foem varchar(20) ,
	fVendor varchar(50) ,
	fRemark varchar(50) ,
	fjk varchar(20) ,
	fHDSize varchar(20) ,
	fsize varchar(20) ,
	fspeed varchar(20) ,
	fCache varchar(20) ,
	fTestTool varchar(50) ,
	HMU varchar(30) ,
	fwarranty int ,
	cDate timestamp ,
	cUser varchar(10) ,
	CartonPN varchar(50) ,
	PackPN varchar(50) ,
	fCutin timestamp ,
	fPhasOut timestamp ,
	fisClosed char(1) ,
	ApproveDate timestamp ,
	User_Approved varchar(10) ,
	fScrapVer varchar(5) ,
	PACKPN_DELL varchar(30) ,
	OEM_LOCATION varchar(50) ,
	Commodity varchar(50),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);


/*触发器*/
create or replace function update_timestamp_cj_HDD_PN() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_HDD_PN on ods_cj_HDD_PN;
create trigger auto_updatetime_cj_HDD_PN
	before update on ods_cj_HDD_PN
	for each row execute procedure update_timestamp_cj_HDD_PN();
/*******************************************************cj_CPU_PN********************************************************/
DROP TABLE IF EXISTS ods_cj_CPU_PN;
CREATE TABLE ods_cj_CPU_PN(
    fpn varchar(6) PRIMARY KEY ,
	fCustDesc varchar(100) ,
	foem varchar(20) ,
	fOemPN varchar(30) ,
	fOemDesc varchar(100) ,
	SCT varchar(10) ,
	HMU varchar(30) ,
	fClass varchar(30) ,
	fName varchar(30) ,
	fModel varchar(50) ,
	ffreq varchar(20) ,
	Cache varchar(10) ,
	MATERIAL varchar(20) ,
	frmaClass int ,
	fver varchar(10) ,
	fWarranty int ,
	cDate timestamp ,
	cUser varchar(10) ,
	fsta int ,
	fPhasOut timestamp ,
	fisClosed char(1) ,
	fPackModel varchar(20) ,
	TestYN char(1) ,
	PDate varchar(20) ,
	fScrapVer varchar(10) ,
	fDate__SafeLunch timestamp ,
	fUser_SafeLunch varchar(10) ,
	PACKPN_DELL varchar(30),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);


/*触发器*/
create or replace function update_timestamp_cj_CPU_PN() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_CPU_PN on ods_cj_CPU_PN;
create trigger auto_updatetime_cj_CPU_PN
	before update on ods_cj_CPU_PN
	for each row execute procedure update_timestamp_cj_CPU_PN();
/*******************************************************CJ_Repair_model********************************************************/
DROP TABLE IF EXISTS ods_cj_Repair_model;
CREATE TABLE ods_cj_Repair_model(
    fpn varchar(30) primary key ,
	fModel varchar(30) ,
	fCustomer varchar(30) ,
	fCommodity varchar(30) ,
	fShipMentPN varchar(30) ,
	fDesc varchar(100) ,
	fMsrVer varchar(10) ,
	fsta int ,
	fRmaClass int ,
	fClass varchar(20) ,
	Standard_Accessary_Material varchar(50) ,
	Carton_PN varchar(20) ,
	Cushion_A varchar(20) ,
	Cushion_B varchar(20) ,
	IW_Return varchar(30) ,
	OW_Return varchar(30) ,
	fWarranty_Month int ,
	fIsNeedNPI char(1) ,
	fDate_Safe_Luanch timestamp ,
	fUser_Safe_Luanch varchar(10) ,
	fOemName varchar(50) ,
	fOem varchar(20) ,
	fDate_RTS varchar(50) ,
	fUser_RTS varchar(10) ,
	fRemark varchar(50) ,
	fCdate timestamp ,
	fCuser varchar(10) ,
	fprcClass varchar(10) ,
	fCutin timestamp ,
	fPhasOut timestamp ,
	fisClosed char(1) ,
	FixtureNO varchar(20) ,
	fDate_Update_Rep timestamp ,
	fUser_Update_Rep varchar(10) ,
	fMatPN varchar(50) ,
	FW varchar(30) ,
	BIOS varchar(20) ,
	PackPn varchar(20) ,
	fScrapVER varchar(10) ,
	fLoc varchar(10) ,
	RepairLevel int ,
	OEM_Model varchar(50) ,
	IsNeedEco boolean ,
	"2Dprefix" varchar(20) ,
	InputV varchar(15) ,
	InputA varchar(10) ,
	Bad_Collect varchar(1) ,
	Remark_Collect varchar(50) ,
	PACKPN_DELL varchar(30),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);
/*触发器*/
create or replace function update_timestamp_cj_Repair_model() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Repair_model on ods_cj_Repair_model;
create trigger auto_updatetime_cj_Repair_model
	before update on ods_cj_Repair_model
	for each row execute procedure update_timestamp_cj_Repair_model();
/*******************************************************CJ_mb_pn********************************************************/
DROP TABLE IF EXISTS ods_cj_mb_pn;
CREATE TABLE ods_cj_mb_pn(
    fpn varchar(20) PRIMARY KEY ,
	foem varchar(20) ,
	fmodel varchar(30) ,
	fdesc varchar(50) ,
	Spec varchar(30) ,
	Warranty int ,
	BGAName varchar(50) ,
	SPC3000 varchar(50) ,
	HMU varchar(50) ,
	SW varchar(30) ,
	cDate timestamp ,
	cuser varchar(10) ,
	fsta int ,
	fver varchar(5) ,
	fCommodity varchar(20) ,
	fisClosed char(1) ,
	CartonPN varchar(50) ,
	PackPN varchar(50) ,
	fCutin timestamp ,
	fPhasOut timestamp ,
	frmaClass int ,
	fDate_Repair_Safe timestamp ,
	fUser_REpair_Safe varchar(10) ,
	fRemark varchar(50) ,
	Platform varchar(50) ,
	BIOS varchar(20) ,
	Battery char(1) ,
	fUpgradePN varchar(10) ,
	fScrapVER varchar(10) ,
	BurnIn_config varchar(50) ,
	Cardbus_type varchar(10) ,
	VGA varchar(10) ,
	HasCPUPN char(1) ,
	IsNeedQT3 char(1) ,
	IsENO char(1) ,
	IsNeedMacCheck char(1) ,
	Path_Pic varchar(50) ,
	IsNeedMasterClean char(1) ,
	IsNeedPhoto char(1) ,
	PhotoPath varchar(150) ,
	SYSID varchar(10) ,
	USCVersion varchar(20) ,
	ePSAVersion varchar(20) ,
	iDRACVersion varchar(20) ,
	BMCVersion varchar(20) ,
	CPLDVersion varchar(20) ,
	IsNeedMsrVersion boolean ,
	IsNeedBIOSVersion boolean ,
	IsNeedUSCVersion boolean ,
	IsNeedePSAVersion boolean ,
	IsNeedIDRACVersion boolean ,
	IsNeedBMCVersion boolean ,
	IsNeedCPLDVersion boolean ,
	IsMACUnique boolean ,
	RR_Scrap int ,
	TPM_TYPE varchar(20) ,
	TPM_ShipYN varchar(1) ,
	CPU_OnBoardYN varchar(1) ,
	CPU_TYPE varchar(50) ,
	CPU_MATPN varchar(50) ,
	SSD_YN varchar(1) ,
	PACKPN_DELL varchar(30) ,
	Platform_Name varchar(50) ,
	SSD_GB varchar(30) ,
	PCH_TYPE varchar(30) ,
	PCH_PN varchar(30) ,
	GPU_TYPE varchar(30) ,
	GPU_PN varchar(30) ,
	Mem_Video_GB varchar(30) ,
	MEM_OnBoardYN varchar(1) ,
	MEM_GB varchar(10) ,
	IsNeedFCB_VER boolean ,
	FCB_VER varchar(20) ,
	IsNeedLOM_VER boolean ,
	LOM_VER varchar(20) ,
	VPRO varchar(1) ,
	TXT varchar(1) ,
	MAC_YN varchar(1) ,
	IsNeedDPK varchar(1) ,
	MSR_ARB varchar(10) ,
	Mac_Num int ,
	Sta_ARB int ,
	fDate_Repair_Safe_ARB timestamp ,
	fUser_REpair_Safe_ARB varchar(10) ,
	MAC_NUM_RJ45 int ,
	MAC_NUM_TYPEC int ,
	MAC_NUM_WIFI int ,
	TPM_TYPE_NEW varchar(10) ,
	OBA_YN varchar(1) ,
	"DPK Rework TC approve" varchar(1),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);

/*触发器*/
create or replace function update_timestamp_cj_mb_pn() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_mb_pn on ods_cj_mb_pn;
create trigger auto_updatetime_cj_mb_pn
	before update on ods_cj_mb_pn
	for each row execute procedure update_timestamp_cj_mb_pn();
/*******************************************************cj_Card_pn********************************************************/
DROP TABLE IF EXISTS ods_cj_Card_pn;
CREATE TABLE ods_cj_Card_pn(
    fpn varchar(6) primary key ,
	fsta int ,
	fRmaClass int ,
	isScreenYN char(1) ,
	fshipPN varchar(6) ,
	fVer varchar(5) ,
	fmodel varchar(30) ,
	fdesc varchar(100) ,
	fClass varchar(20) ,
	foem varchar(20) ,
	fVendor varchar(50) ,
	fRemark varchar(50) ,
	fwarranty int ,
	cDate timestamp ,
	cUser varchar(10) ,
	CartonPN varchar(50) ,
	PackPN varchar(50) ,
	fCutin timestamp ,
	fPhasOut timestamp ,
	fisClosed char(1) ,
	ApproveDate timestamp ,
	ApproveUser varchar(10) ,
	fScrapVer varchar(5) ,
	fw varchar(20) ,
	Bios varchar(60) ,
	fCommodity varchar(30) ,
	Cardbus_type varchar(20) ,
	VGA varchar(20) ,
	HMU varchar(50) ,
	SPC3000 varchar(50) ,
	IsNeedIDT varchar(1) ,
	fTools varchar(50) ,
	ReturnOEM varchar(30) ,
	OEM_PN varchar(20) ,
	PLT varchar(1) ,
	THRM varchar(1) ,
	Remark_QA varchar(100) ,
	PACKPN_DELL varchar(30),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);

/*触发器*/
create or replace function update_timestamp_cj_Card_pn() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Card_pn on ods_cj_Card_pn;
create trigger auto_updatetime_cj_Card_pn
	before update on ods_cj_Card_pn
	for each row execute procedure update_timestamp_cj_Card_pn();
/*******************************************************base_pn_docking********************************************************/
DROP TABLE IF EXISTS ods_base_pn_docking;
CREATE TABLE ods_base_pn_docking(
    fPN varchar(30) ,
	SvrCode int ,
	fMODEL varchar(50) ,
	fdesc varchar(50) ,
	Platform varchar(50) ,
	fOEM varchar(30) ,
	Commodity varchar(30) ,
	Warranty varchar(10) ,
	BattYN varchar(10) ,
	fsta int ,
	fMsrVer varchar(10) ,
	FW varchar(50) ,
	IsNeedMacCheck varchar(1) ,
	IsMACUnique varchar(1) ,
	IsNeedSTCheck varchar(1) ,
	PackPN varchar(20) ,
	PACKPN_DELL varchar(20) ,
	fUpgradePN varchar(20) ,
	cDate timestamp ,
	cuser varchar(10) ,
	Path_Pic varchar(100) ,
	fDate_Safe_Luanch timestamp ,
	fUser_Safe_Luanch varchar(10) ,
	fScrapVER varchar(10) ,
	fPhasOut timestamp ,
	fCdate timestamp ,
	fCuser varchar(10) ,
	Full_PN varchar(10) ,
	Cable_PN varchar(50),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fPN);
/*触发器*/
create or replace function update_timestamp_base_pn_docking() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_base_pn_docking on ods_base_pn_docking;
create trigger auto_updatetime_base_pn_docking
	before update on ods_base_pn_docking
	for each row execute procedure update_timestamp_base_pn_docking();
/*******************************************************Screen_Disposition********************************************************/
DROP TABLE IF EXISTS ods_Screen_Disposition;
CREATE TABLE ods_Screen_Disposition(
    id int,
	fSvrCode int ,
	fsta int ,
	fScreenID int ,
	IQC varchar(5) ,
	VI varchar(5) ,
	QT1 varchar(5) ,
	BIOS char(1) ,
	ECO char(1) ,
	QT2 varchar(5) ,
	FCT varchar(5) ,
	FT varchar(5) ,
	QT3 varchar(5) ,
	Disposition varchar(5) ,
	Disposition2 varchar(10) ,
	fErrDesc varchar(500) ,
	fFailReason varchar(500) ,
	fAction varchar(100) ,
	fErrCode varchar(200) ,
	fFailReasonCode varchar(200) ,
	cDate timestamp ,
	fDate_Disposition timestamp ,
	fBGADateCode varchar(50) ,
	FP_Error_Code varchar(100) ,
	FP_Error_Discription varchar(200) ,
	TestStep int ,
	Component_Removed_Description1 varchar(2000) ,
	Component_Removed_Vendor1 varchar(2000) ,
	Component_Removed_Vendor_Part_Number1 varchar(2000) ,
	Component_Removed_Reference_Designator1 varchar(2000) ,
	Repair_Associated_Module1 varchar(200) ,
	Component_Removed_Description2 varchar(50) ,
	Component_Removed_Vendor2 varchar(50) ,
	Component_Removed_Vendor_Part_Number2 varchar(30) ,
	Component_Removed_Reference_Designator2 varchar(20) ,
	Repair_Associated_Module2 varchar(50) ,
	Component_Removed_Description3 varchar(50) ,
	Component_Removed_Vendor3 varchar(50) ,
	Component_Removed_Vendor_Part_Number3 varchar(30) ,
	Component_Removed_Reference_Designator3 varchar(20) ,
	Repair_Associated_Module3 varchar(50) ,
	Scrap varchar(30) ,
	Repair_Attempts int ,
	Quick_Test_Count int ,
	Functional_Test_count int ,
	Run_In_count int ,
	Final_Test_count int ,
	OBA_count int ,
	"Failure Type" varchar(30) ,
	"Components Causing Scrap" varchar(100) ,
	"BID Count" int ,
	TestID_VMI int ,
	TestID_QT1 int ,
	TestID_FCT int ,
	TestID_FA int ,
	TestID_OBA int ,
	TestID_FVMI int,
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fScreenID);
/*触发器*/
create or replace function update_timestamp_Screen_Disposition() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_Screen_Disposition on ods_Screen_Disposition;
create trigger auto_updatetime_Screen_Disposition
	before update on ods_Screen_Disposition
	for each row execute procedure update_timestamp_Screen_Disposition();


/*******************************************************wx_Wait_master********************************************************/
DROP TABLE IF EXISTS ods_wx_Wait_master;
CREATE TABLE ods_wx_Wait_master(
    id int ,
	fRmaClass int ,
	fItemid int ,
	fPostSta int ,
	Cdate timestamp ,
	CUser varchar(20) ,
	fWaitType int ,
	fWaitReasonId int ,
	fWaitReason varchar(50) ,
	fRemark varchar(200) ,
	fUnWaitDate timestamp ,
	fUnWaitUser varchar(20) ,
	fsta int,
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (id);
/*触发器*/
create or replace function update_timestamp_wx_Wait_master() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_wx_Wait_master on ods_wx_Wait_master;
create trigger auto_updatetime_wx_Wait_master
	before update on ods_wx_Wait_master
	for each row execute procedure update_timestamp_wx_Wait_master();
/*******************************************************cj_Delay_Mast********************************************************/
DROP TABLE IF EXISTS ods_cj_Delay_Mast;
CREATE TABLE ods_cj_Delay_Mast(
    fcode int ,
	flevel int ,
	fCodeName varchar(50) ,
	fDesc varchar(100) ,
	fCodeName_EN varchar(50) ,
	fSysCtrl char(1) ,
	fDelayOwner varchar(50) ,
	fAllowDis char(1) ,
	fIsMulPost char(1) ,
	fIsMulDis char(1) ,
	Monitor char(1) ,
	Panel char(1) ,
	PSU char(1) ,
	MEM char(1) ,
	PRN char(1) ,
	PRJ char(1) ,
	MB char(1) ,
	HD char(1) ,
	ODD char(1) ,
	RMA char(1) ,
	TV char(1) ,
	AWP char(1) ,
	DellMB char(1) ,
	cDate timestamp ,
	cUser varchar(20) ,
	InHouseType int ,
	DelayCategory varchar(50) ,
	DelayType varchar(50),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fcode);
/*触发器*/
create or replace function update_timestamp_cj_Delay_Mast() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Delay_Mast on ods_cj_Delay_Mast;
create trigger auto_updatetime_cj_Delay_Mast
	before update on ods_cj_Delay_Mast
	for each row execute procedure update_timestamp_cj_Delay_Mast();


/*******************************************************wx_Monitor********************************************************/
DROP TABLE IF EXISTS ods_wx_Monitor;
CREATE TABLE ods_wx_Monitor(
    id int ,
	fpn varchar(20) ,
	Platform varchar(50) ,
	fDesc varchar(50) ,
	fPanelType int ,
	fOem varchar(20) ,
	fMinVer varchar(50) ,
	fWarranty int ,
	fPbfree varchar(1) ,
	fCutin timestamp ,
	fPhasOut timestamp ,
	fDBDesc varchar(200) ,
	fDCnt int ,
	fUpdate timestamp ,
	fuser varchar(10) ,
	fmSta int ,
	Capability_Level varchar(50) ,
	ApproveDate timestamp ,
	User_Approved varchar(10) ,
	fSelf_inicure char(1) ,
	fProduct_status varchar(20) ,
	fFW varchar(20) ,
	CartonPN varchar(20) ,
	CushionA varchar(10) ,
	CushionB varchar(10) ,
	CushionC varchar(10) ,
	fisPanelMPI char(1) ,
	fPanel_supplier varchar(20) ,
	fPurUser varchar(20) ,
	fPurdate timestamp ,
	Cdate timestamp ,
	CUser varchar(10) ,
	fisClosed char(1) ,
	fisAllowScrap char(1) ,
	fNotAllowReason varchar(50) ,
	fType int ,
	isNUDD char(1) ,
	splx varchar(20) ,
	isAIO varchar(1) ,
	fsize varchar(10) ,
	fcustomer varchar(50) ,
	isGood char(1) ,
	IsNeedSvrTag char(1) ,
	Oversea_part varchar(5) ,
	Cable_PN varchar(30) ,
	NUDD_Remark varchar(50) ,
	fFWTool varchar(200) ,
	fFW_PD varchar(30) ,
	fFW_Bridge varchar(30) ,
	fsta_ARB int ,
	Date_SafeLunch_ARB timestamp ,
	User_SafeLunch_ARB varchar(20) ,
	MSR_ARB varchar(10) ,
	DC_V numeric(10, 2) ,
	DC_mA numeric(10, 2),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);
/*触发器*/
create or replace function update_timestamp_wx_Monitor() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_wx_Monitor on ods_wx_Monitor;
create trigger auto_updatetime_wx_Monitor
	before update on ods_wx_Monitor
	for each row execute procedure update_timestamp_wx_Monitor();
/*******************************************************wx_item********************************************************/
DROP TABLE IF EXISTS ods_wx_item;
CREATE TABLE ods_wx_item(
    id int,
	fsta int ,
	fbSta int ,
	fResCode int ,
	fClassID int ,
	FItemID int ,
	fmodel varchar(50) ,
	fTprSN varchar(20) ,
	fVer varchar(15) ,
	fReceiveDate timestamp ,
	fReceiveVer varchar(15) ,
	fBios varchar(20) ,
	fppid varchar(50) ,
	fpn varchar(10) ,
	fReturnto varchar(20) ,
	fcon varchar(30) ,
	fWarranty varchar(4) ,
	fReTimes int ,
	fReMark varchar(200) ,
	fECN int ,
	CDate varchar(20) ,
	fProDate varchar(20) ,
	fVerChk char(1) ,
	fIsNewPart char(1) ,
	fIsNeedECR char(1) ,
	fECRLable varchar(5) ,
	fEngineer varchar(10) ,
	fNextSta int ,
	fIsNeedQA char(1) ,
	fOBARno varchar(20) ,
	fHasOBA boolean,
	fRepairRes int ,
	fSeq int ,
	fAttRemark varchar(100) ,
	fLoc varchar(50) ,
	fECNYN char(1) ,
	fEcnNo varchar(50) ,
	fEcnDate varchar(20) ,
	fVIMRes int ,
	fHasAssigned char(1) ,
	fAssignDate varchar(20) ,
	fDate_wxClose varchar(20) ,
	fDate_Close varchar(20) ,
	fDate_Post varchar(20) ,
	fUser_post varchar(20) ,
	fDate_postDell varchar(20) ,
	fDate_DellConf varchar(20) ,
	fDate_Scrap varchar(20) ,
	fRmaID int ,
	fRmaCkID int ,
	frma varchar(30) ,
	fDELLID int ,
	SCID int ,
	fReceiver varchar(10) ,
	fLastID int ,
	fDate_LastClosed varchar(20) ,
	fRemark_P varchar(100) ,
	fIsReplace boolean ,
	fErrDesc varchar(100) ,
	ftestStep int ,
	fFailureType int ,
	fFailureTypeRemark varchar(50) ,
	fDelayReason varchar(50) ,
	fDelayCode int ,
	fwaitid int ,
	fBurnHr int ,
	fUsageTime numeric(18, 2) ,
	fPanelTimes int ,
	fPanelFAE int ,
	fpanelid int ,
	fVIMChecker varchar(10) ,
	fLM float ,
	flm_supper varchar(20) ,
	fPanelRepType int ,
	fisMatRepair char(1) ,
	fbadSta int ,
	fMBSn varchar(30) ,
	fFAType int ,
	fLcdYN char(1) ,
	fPanelPPID varchar(50) ,
	fPanelSn varchar(50) ,
	fPanelVer varchar(10) ,
	fPanelModel varchar(50) ,
	fPanelPPID_Dell varchar(50) ,
	fjyd float ,
	fVimCheck char(1) ,
	fFunCheck char(1) ,
	fPRC varchar(50) ,
	fShipBox varchar(11) ,
	fso varchar(20) ,
	fIsRepairYN char(1) ,
	fErrDescID int ,
	fScrap_BR varchar(3) ,
	isNeedAssign char(1) ,
	fAssignID int ,
	fDate_Burn timestamp ,
	fDate_FinalTest timestamp ,
	fFanxinDelaySta char(1) ,
	fUnRepairReason varchar(150) ,
	fischaij varchar(20) ,
	fisNtoScrap varchar(1) ,
	SvrTag varchar(30) ,
	ETA timestamp ,
	Label_DSP varchar(50) ,
	ZX_MatPN varchar(50) ,
	WH_ID int ,
	WH_sta int ,
	WH_BINCode varchar(50) ,
	WH_DATE_IN timestamp ,
	WH_USER_IN varchar(50) ,
	WH_DATE_OUT timestamp ,
	WH_OWNER_OUT varchar(20) ,
	WH_USER_OUT varchar(10) ,
	Date_RepairCheck timestamp ,
	edid varchar(20) ,
	edidModel varchar(20) ,
	HaveKS boolean ,
	HaveGlass boolean ,
	ZX_MatOld char(1) ,
	NewPanel_ID int ,
	fOBADate timestamp ,
	Date_Shipping timestamp ,
	AllowOtherScrap varchar(30) ,
	HopeDate timestamp ,
	EasyPlanDate timestamp ,
	EasyPlanNO varchar(10) ,
	BinItem_fNo varchar(30) ,
	BinItem_fsta int ,
	TJZP varchar(1) ,
	Pre_PlanDate timestamp ,
	Pre_PlanUser varchar(30) ,
	QACheckResult char(1) ,
	QACheckDate timestamp ,
	QACheckUser varchar(20) ,
	QACheckMemo varchar(100) ,
	QAToJJDate timestamp ,
	QAToJJUser varchar(20) ,
	fFw varchar(20) ,
	FirstNPI boolean ,
	TotTAT int ,
	NowTAT int ,
	NPI_YN varchar(1) ,
	fFW_PD varchar(30) ,
	fFW_Bridge varchar(30) ,
	BID_Cnt int ,
	fBurnHr_VMI int ,
	fDate_OutFromHouse_Plan timestamp ,
	fUser_OutFromHouse_Plan varchar(10) ,
	CoMat_Type varchar(50),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (id);
/*触发器*/
create or replace function update_timestamp_wx_item() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_wx_item on ods_wx_item;
create trigger auto_updatetime_wx_item
	before update on ods_wx_item
	for each row execute procedure update_timestamp_wx_item();

UPDATE ods_wx_item
    set updatetime=(now() - interval '1 day')
where updatetime::date='2021-7-22'::date;
/*******************************************************cj_Mem_Model********************************************************/
DROP TABLE IF EXISTS ods_cj_Mem_Model;
CREATE TABLE ods_cj_Mem_Model(
   fpn varchar(5) ,
	foem varchar(20) ,
	fmodel varchar(30) ,
	fdesc varchar(50) ,
	ODM varchar(30) ,
	Category varchar(50) ,
	Capacity varchar(30) ,
	Spec varchar(30) ,
	Warranty int ,
	BGAName varchar(50) ,
	SPC3000 varchar(50) ,
	HMU varchar(50) ,
	SW varchar(30) ,
	fisEcc char(1) ,
	cDate timestamp ,
	cuser varchar(10) ,
	fsta int ,
	fver varchar(5) ,
	fCommodity varchar(20) ,
	fisClosed char(1) ,
	CartonPN varchar(50) ,
	PackPN varchar(50) ,
	GWGG varchar(30) ,
	KLGG varchar(30) ,
	fCutin timestamp ,
	fPhasOut timestamp ,
	frmaClass int ,
	fDate_BGA_Safe timestamp ,
	fUser_BGA_Safe varchar(10) ,
	fDate_Repair_Safe timestamp ,
	fUser_REpair_Safe varchar(10) ,
	fRemark varchar(50) ,
	SPDVer varchar(20) ,
	fScrapVer varchar(5) ,
	ZY_PN varchar(1) ,
	Quality_issue varchar(1) ,
	PACKPN_DELL varchar(30),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fmodel);
/*触发器*/
create or replace function update_timestamp_cj_Mem_Model() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Mem_Model on ods_cj_Mem_Model;
create trigger auto_updatetime_cj_Mem_Model
	before update on ods_cj_Mem_Model
	for each row execute procedure update_timestamp_cj_Mem_Model();
/*******************************************************ODM_RMA_ITEM********************************************************/
DROP TABLE IF EXISTS ods_ODM_RMA_ITEM;
CREATE TABLE ods_ODM_RMA_ITEM(
    id int ,
	SvrCode int ,
	Rmaid int ,
	BillID int ,
	RequestID int ,
	fseq int ,
	RequestNo varchar(50) ,
	fRma varchar(50) ,
	fcon varchar(50) ,
	PO varchar(50) ,
	SO varchar(50) ,
	cDate timestamp ,
	cUser varchar(10) ,
	Date_Request timestamp ,
	Date_issued timestamp ,
	User_issued varchar(10) ,
	Date_signing timestamp ,
	User_signing varchar(10) ,
	Date_Receive timestamp ,
	User_Receive varchar(10) ,
	ReturnTimes int ,
	Depot varchar(20) ,
	fpn varchar(30) ,
	ReplacePN varchar(30) ,
	fsta int ,
	fNextSta int ,
	fRescode int ,
	fResult_VMI int ,
	fResult int ,
	fRemark varchar(50) ,
	fRemark_Repair varchar(50) ,
	Returnto varchar(30) ,
	PPID varchar(50) ,
	SN varchar(50) ,
	VendorSN varchar(50) ,
	RequestType varchar(10) ,
	RecVer varchar(10) ,
	Ver varchar(10) ,
	Date_HandOverIn timestamp ,
	User_HandOverIn varchar(10) ,
	Date_Screen timestamp ,
	User_Screen varchar(10) ,
	Depto_PN varchar(30) ,
	Depto_PPID varchar(50) ,
	Date_Repair timestamp ,
	User_Repair varchar(10) ,
	Date_FinalTest timestamp ,
	User_FinalTest varchar(10) ,
	Date_Packing timestamp ,
	User_Packing varchar(10) ,
	Date_Closed timestamp ,
	User_Closed varchar(10) ,
	LastID int ,
	IsECR varchar(1) ,
	Date_ECR timestamp ,
	User_ECR varchar(10) ,
	ShipPN varchar(30) ,
	fOBA_No varchar(30) ,
	fOBA_Date timestamp ,
	fOBA_User varchar(20) ,
	fOBA_BadReturnDate timestamp ,
	fOBA_Result varchar(10) ,
	IsDOA varchar(1) ,
	IsOnhold varchar(1) ,
	OnHoldID int ,
	MAC varchar(50) ,
	SRBoxID int ,
	fBin varchar(20) ,
	Date_Diss timestamp ,
	User_Diss varchar(10) ,
	PanelRepairYN varchar(1) ,
	PanelModel varchar(50) ,
	PanelSn varchar(50) ,
	PanelSn_NEW varchar(50) ,
	PanelRepairType int ,
	PanelVendor varchar(30) ,
	ImportID int ,
	Warranty varchar(10) ,
	RPN varchar(20) ,
	DateCode varchar(20) ,
	Date_M timestamp ,
	ErrorCode_VMI varchar(100) ,
	Error_VMI varchar(300) ,
	Date_BadPost timestamp ,
	User_BadPost varchar(10) ,
	Sta_BadPost int ,
	Date_RmaCancel timestamp ,
	User_RmaCancel varchar(10) ,
	Sta_RmaCancel int ,
	BatchNo int ,
	LastCID_Rma varchar(50) ,
	LastCID_ID int ,
	SHIP_POID int ,
	PO_Verify varchar(1) ,
	PO_Verify_Date timestamp ,
	TestID_VMI int ,
	Loc_Sta int ,
	Loc_Date_in timestamp ,
	Loc_User_in varchar(10) ,
	Loc_Date_out timestamp ,
	Loc_User_out varchar(10) ,
	Loc_Remark varchar(50) ,
	Loc_Owner_out varchar(10) ,
	TestID_FA int ,
	TprSn varchar(50) ,
	WH_ItemID int ,
	TestID_Curr int ,
	Scrap_Type int ,
	PO_position int ,
	PO_SHIP varchar(50) ,
	SO_SHIP varchar(50) ,
	PO_position_SHIP int ,
	DissID_LCM int ,
	DissID_TP int ,
	CountryCode varchar(30) ,
	SvrTag varchar(30) ,
	TAG varchar(20) ,
	fPrice decimal(19,4) ,
	IMPORTANT_NOTE varchar(50) ,
	Date_ODM timestamp ,
	Depot_SR varchar(30) ,
	Burnin_H int ,
	Unit_Cosmetic_Condition varchar(50) ,
	Packaging_Cosmetic_Condition varchar(50) ,
	OLDSYS_ID int ,
	Date_Closed_Last timestamp ,
	Lab_Panel varchar(20) ,
	Lab_BL varchar(20) ,
	Lab_Use char(1) ,
	Two90day boolean ,
	EasyPlanDate timestamp ,
	Lab_10 varchar(10) ,
	Lab_10Use char(1) ,
	MonitorPPID varchar(50) ,
	EasyPlanNO varchar(10) ,
	ReciveType int ,
	CustomerBarCode varchar(50) ,
	CustomerNewBarCode varchar(20) ,
	TConSn varchar(30) ,
	Synchro_Rev_SH boolean ,
	TV_Level varchar(10) ,
	HopeDate timestamp ,
	WorkGroup varchar(30) ,
	Geo varchar(30) ,
	SendDiss boolean ,
	SendDissTime timestamp ,
	FirstDate timestamp ,
	Priority_Level int ,
	Ticket_ID varchar(30) ,
	Date_ETD timestamp ,
	Allow_21 char(1) ,
	Depot_OutRepair varchar(30) ,
	HopeLevel varchar(10) ,
	Send_Auto varchar(10) ,
	PackingBoxYN varchar(1) ,
	HS_ID_Receive int ,
	BinItem_fNo varchar(30) ,
	BinItem_fsta int ,
	SendDissUser varchar(30) ,
	Pre_PlanDate timestamp ,
	Pre_PlanUser varchar(30) ,
	JStoJJ_User varchar(20) ,
	JStoJJ_time timestamp ,
	FW varchar(50) ,
	EDID varchar(30) ,
	QACheckResult char(1) ,
	QACheckDate timestamp ,
	QACheckUser varchar(20) ,
	QACheckMemo varchar(100) ,
	QAToJJDate timestamp ,
	QAToJJUser varchar(20) ,
	DOACheckType varchar(10) ,
	VMI_BOM_ChkYN varchar(1) ,
	AllowOpenCell varchar(1) ,
	Lab_Tray varchar(30) ,
	Lab_Pallet varchar(30) ,
	Lab_ASUS_CSN varchar(30) ,
	fOQC_No varchar(30) ,
	fOQC_Date timestamp ,
	fOQC_User varchar(20) ,
	BondJJ_time timestamp ,
	BondJJ_User varchar(30) ,
	Check16JJ_time timestamp ,
	Check16JJ_User varchar(30) ,
	Check21JJ_time timestamp ,
	Check21JJ_User varchar(30) ,
	BBoxid int ,
	BBoxTime timestamp ,
	BBoxUser varchar(30) ,
	TotTAT int ,
	NowTAT int ,
	FirstNPI boolean ,
	CheckJJ_OBA_time timestamp ,
	CheckJJ_OBA_User varchar(30) ,
	fOBA_Pass_Date timestamp ,
	TPCBSn varchar(30) ,
	Allow3RetimesWx boolean ,
	AllowLenovoPNLine boolean ,
	TPSN varchar(50) ,
	Panel_RecVer varchar(10) ,
	Panel_Ver varchar(10) ,
	TSP_RecVer varchar(10) ,
	TSP_Ver varchar(10) ,
	fbin_Full boolean ,
	IsOLED varchar(10) ,
	OCSN varchar(10) ,
	Lenovo_TempArea varchar(200),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (id);
/*触发器*/
create or replace function update_timestamp_ODM_RMA_ITEM() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_ODM_RMA_ITEM on ods_ODM_RMA_ITEM;
create trigger auto_updatetime_ODM_RMA_ITEM
	before update on ods_ODM_RMA_ITEM
	for each row execute procedure update_timestamp_ODM_RMA_ITEM();
/*******************************************************cj_PANEL_code********************************************************/
DROP TABLE IF EXISTS ods_cj_PANEL_code;
CREATE TABLE ods_cj_PANEL_Code(
    id int ,
	FType int ,
	FTypeName varchar(50) ,
	FCode int ,
	FCodeName varchar(100) ,
	FParent varchar(20),
	updatetime timestamp default current_timestamp
)
Distributed by (id);
/*触发器*/
create or replace function update_timestamp_cj_PANEL_code() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_PANEL_code on ods_cj_PANEL_code;
create trigger auto_updatetime_cj_PANEL_code
	before update on ods_cj_PANEL_code
	for each row execute procedure update_timestamp_cj_PANEL_code();
/*******************************************************ODM_PN_TPanel********************************************************/
DROP TABLE IF EXISTS ods_ODM_PN_TPanel;
CREATE TABLE ods_ODM_PN_TPanel(
    skey varchar(50) ,
	fpn varchar(20) ,
	fsta int ,
	SvrCode int ,
	fModel varchar(20) ,
	fSize_1 varchar(20) ,
	fSize varchar(20) ,
	fDesc varchar(200) ,
	OEM varchar(20) ,
	Returnto varchar(20) ,
	DirectingBonding varchar(30) ,
	Date_SafeLunch timestamp ,
	User_SafeLunch varchar(20) ,
	Date_AnsyClosed timestamp ,
	User_AnsyClosed varchar(10) ,
	ODM varchar(30) ,
	TouchScreen varchar(50) ,
	Pure_AssyPanel varchar(50) ,
	BOM varchar(1) ,
	Panel_Supplier varchar(30) ,
	Platform varchar(300) ,
	fPhasOut timestamp ,
	fisClosed varchar(1) ,
	cdate timestamp ,
	cUser varchar(20) ,
	PackPN varchar(20) ,
	IsPanelRepair varchar(1) ,
	Warranty int ,
	ReplacePN varchar(50) ,
	RTS timestamp ,
	HW varchar(20) ,
	CJPN varchar(30) ,
	LCFCPN varchar(50) ,
	ManufacturedSite varchar(50) ,
	PanelModel varchar(2000) ,
	NetWeigth numeric(18, 4) ,
	Series varchar(30) ,
	RTV_Returnto varchar(50) ,
	HMU_Model varchar(100) ,
	OEM_MODEL varchar(50) ,
	fGroup varchar(50) ,
	Product varchar(50) ,
	fbl varchar(50) ,
	attach varchar(50) ,
	pgp varchar(50) ,
	ACF varchar(50) ,
	Community varchar(50) ,
	RepairDepot varchar(30) ,
	Sta_Repair int ,
	Price_Repair decimal(19,4),
	ROD varchar(1) ,
	Date_Repair_apply timestamp ,
	Date_Repair_conf timestamp ,
	User_Repair_Conf varchar(10) ,
	ECR_Sta int ,
	ECR_Sta_Update timestamp ,
	OEM_TP varchar(30) ,
	DissCategory_Group int ,
	MSR varchar(10) ,
	BIT int ,
	VDD decimal(10, 4) ,
	Min_luminance int ,
	Wx_min decimal(10, 4) ,
	Wx_max decimal(10, 4) ,
	Wy_min decimal(10, 4) ,
	Wy_max decimal(10, 4) ,
	NPISourceId int ,
	TouchTechnology varchar(100) ,
	PlasticFrameTechnology varchar(100) ,
	MianBom varchar(200) ,
	PanelTest varchar(100) ,
	FlowId int ,
	Uniformity float ,
	UpgradePN varchar(30) ,
	DissChangPPID boolean ,
	SupplierCode varchar(100) ,
	IsTwo_In_One varchar(1) ,
	In21_AB varchar(10) ,
	In21_Dglue varchar(10) ,
	In21_LED varchar(10) ,
	In21_Dlight varchar(10) ,
	In21_ProUser varchar(30) ,
	In21_ProDate timestamp ,
	In21_FaUser varchar(30) ,
	In21_FaDate timestamp ,
	In21_FirstID int ,
	EDID_CheckSum varchar(20) ,
	TPSurfaceTechnology varchar(50) ,
	UM varchar(50) ,
	AShell varchar(20) ,
	TestAccessories varchar(100) ,
	HasFrontCover char(2) ,
	LocaUse decimal(10, 2) ,
	ZKUse decimal(10, 2) ,
	AKUse decimal(10, 2) ,
	Ldiag_TestYN char(1) ,
	fsta_ARB int ,
	Date_SafeLunch_ARB timestamp ,
	User_SafeLunch_ARB varchar(20) ,
	MSR_ARB varchar(10) ,
	OBA_YN varchar(1) ,
	CellThickness varchar(10) ,
	CoverThickness varchar(10) ,
	IsOLED varchar(10) ,
	AShell_Material varchar(100) ,
	AShell_Machining_Technology varchar(100),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fpn);
/*触发器*/
create or replace function update_timestamp_ODM_PN_TPanel() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_ODM_PN_TPanel on ods_ODM_PN_TPanel;
create trigger auto_updatetime_ODM_PN_TPanel
	before update on ods_ODM_PN_TPanel
	for each row execute procedure update_timestamp_ODM_PN_TPanel();
/*******************************************************Panel_Screen_Disposition********************************************************/
DROP TABLE IF EXISTS ods_Panel_Screen_Disposition;
CREATE TABLE ods_Panel_Screen_Disposition(
    id int ,
	fSvrCode int ,
	fsta int ,
	fScreenID int ,
	IQC varchar(5) ,
	VI varchar(5) ,
	QT1 varchar(5) ,
	BIOS char(1) ,
	ECO char(1) ,
	QT2 varchar(5) ,
	FCT varchar(5) ,
	FT varchar(5) ,
	QT3 varchar(5) ,
	Disposition varchar(5) ,
	Disposition2 varchar(10) ,
	fErrDesc varchar(500) ,
	fFailReason varchar(500) ,
	fAction varchar(100) ,
	fErrCode varchar(200) ,
	fFailReasonCode varchar(200) ,
	cDate timestamp ,
	fDate_Disposition timestamp ,
	fBGADateCode varchar(50) ,
	FP_Error_Code varchar(100) ,
	FP_Error_Discription varchar(200) ,
	TestStep int ,
	Component_Removed_Description1 varchar(2000) ,
	Component_Removed_Vendor1 varchar(2000) ,
	Component_Removed_Vendor_Part_Number1 varchar(2000) ,
	Component_Removed_Reference_Designator1 varchar(2000) ,
	Repair_Associated_Module1 varchar(200) ,
	Component_Removed_Description2 varchar(50) ,
	Component_Removed_Vendor2 varchar(50) ,
	Component_Removed_Vendor_Part_Number2 varchar(30) ,
	Component_Removed_Reference_Designator2 varchar(20) ,
	Repair_Associated_Module2 varchar(50) ,
	Component_Removed_Description3 varchar(50) ,
	Component_Removed_Vendor3 varchar(50) ,
	Component_Removed_Vendor_Part_Number3 varchar(30) ,
	Component_Removed_Reference_Designator3 varchar(20) ,
	Repair_Associated_Module3 varchar(50) ,
	Scrap varchar(10) ,
	Repair_Attempts int ,
	Quick_Test_Count int ,
	Functional_Test_count int ,
	Run_In_count int ,
	Final_Test_count int ,
	OBA_count int ,
	"Failure Type" varchar(30) ,
	"Components Causing Scrap" varchar(100) ,
	"BID Count" int,
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fScreenID);
/*触发器*/
create or replace function update_timestamp_Panel_Screen_Disposition() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_Panel_Screen_Disposition on ods_Panel_Screen_Disposition;
create trigger auto_updatetime_Panel_Screen_Disposition
	before update on ods_Panel_Screen_Disposition
	for each row execute procedure update_timestamp_Panel_Screen_Disposition();
/*******************************************************ODM_OnHold_Item********************************************************/
DROP TABLE IF EXISTS ods_ODM_OnHold_Item;
CREATE TABLE ods_ODM_OnHold_Item(
    id int ,
	RmaItemid int ,
	OnHoldType int ,
	fDesc varchar(100) ,
	fRemark varchar(50) ,
	fsta int ,
	cDate timestamp ,
	cUser varchar(10) ,
	Date_Dis timestamp ,
	User_Dis varchar(10) ,
	fbit varchar(20) ,
	fqty int ,
	Qty_OnHand int ,
	Returnto varchar(30) ,
	MatModel varchar(50) ,
	Sta_TB int ,
	Date_TB timestamp ,
	MatDissYN varchar(1) ,
	Date_MatDissYN timestamp ,
	ProUser varchar(10) ,
	ProDate timestamp ,
	FailReason varchar(50) ,
	Date_Diss_Per timestamp ,
	User_Diss_Per varchar(10),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (id);
/*触发器*/
create or replace function update_timestamp_ODM_OnHold_Item() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_ODM_OnHold_Item on ods_ODM_OnHold_Item;
create trigger auto_updatetime_ODM_OnHold_Item
	before update on ods_ODM_OnHold_Item
	for each row execute procedure update_timestamp_ODM_OnHold_Item();
/*******************************************************Base_ODM_OnHold********************************************************/
DROP TABLE IF EXISTS ods_Base_ODM_OnHold;
CREATE TABLE ods_Base_ODM_OnHold(
    fcode int ,
	flevel int ,
	fCodeName varchar(50) ,
	fDesc varchar(100) ,
	fCodeName_EN varchar(50) ,
	fSysCtrl char(1) ,
	fDelayOwner varchar(50) ,
	fAllowDis char(1) ,
	AWP char(1) ,
	cDate timestamp ,
	cUser varchar(20) ,
	DelayCategory varchar(50) ,
	DelayType varchar(50),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (fcode);
/*触发器*/
create or replace function update_timestamp_Base_ODM_OnHold() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_Base_ODM_OnHold on ods_Base_ODM_OnHold;
create trigger auto_updatetime_Base_ODM_OnHold
	before update on ods_Base_ODM_OnHold
	for each row execute procedure update_timestamp_Base_ODM_OnHold();
/*******************************************************wx_Disposition********************************************************/
DROP TABLE IF EXISTS ods_wx_Disposition;
CREATE TABLE ods_wx_Disposition(
   id int ,
   fsta int ,
   fwxID int ,
   IQC varchar(5) ,
   VI varchar(5) ,
   QT1 varchar(5) ,
   BIOS char(1) ,
   ECO char(1) ,
   QT2 varchar(5) ,
   FCT varchar(5) ,
   FT varchar(5) ,
   QT3 varchar(5) ,
   Disposition varchar(5) ,
   Disposition2 varchar(10) ,
   fErrDesc varchar(500) ,
   fFailReason varchar(500) ,
   fAction varchar(100) ,
   fErrCode varchar(200) ,
   fFailReasonCode varchar(200) ,
   cDate timestamp ,
   fDate_Disposition timestamp ,
   fBGADateCode varchar(50) ,
   FP_Error_Code varchar(100) ,
   FP_Error_Discription varchar(200) ,
   TestStep int ,
   fFailType varchar(20) ,
   Component_Removed_Description1 varchar(2000) ,
   Component_Removed_Vendor1 varchar(2000) ,
   Component_Removed_Vendor_Part_Number1 varchar(2000) ,
   Component_Removed_Reference_Designator1 varchar(2000) ,
   Repair_Associated_Module1 varchar(200) ,
   Component_Removed_Description2 varchar(50) ,
   Component_Removed_Vendor2 varchar(50) ,
   Component_Removed_Vendor_Part_Number2 varchar(30) ,
   Component_Removed_Reference_Designator2 varchar(20) ,
   Repair_Associated_Module2 varchar(50) ,
   Component_Removed_Description3 varchar(50) ,
   Component_Removed_Vendor3 varchar(50) ,
   Component_Removed_Vendor_Part_Number3 varchar(30) ,
   Component_Removed_Reference_Designator3 varchar(20) ,
   Repair_Associated_Module3 varchar(50) ,
   Scrap varchar(30) ,
   Panel_ODM varchar(30) ,
   "Failure Type" varchar(30) ,
   Repair_Attempts int ,
   Quick_Test_Count int ,
   Functional_Test_count int ,
   Run_In_count int ,
   Final_Test_count int ,
   OBA_count int ,
   "Components Causing Scrap" varchar(100) ,
   "BID Count" int,
   updatetime timestamp default (now() - interval '1 day')
)Distributed by (id);

/*触发器*/
create or replace function update_timestamp_wx_Disposition() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_wx_Disposition on ods_wx_Disposition;
create trigger auto_updatetime_wx_Disposition
	before update on ods_wx_Disposition
	for each row execute procedure update_timestamp_wx_Disposition();

update ods_wx_Disposition
set updatetime=( now() - interval '1 day')
where updatetime::date=current_timestamp::date;
/*******************************************************Commodity_Color_Aging_Model********************************************************/
DROP TABLE IF EXISTS ods_Commodity_Color_Aging_Model;
CREATE TABLE ods_Commodity_Color_Aging_Model(
    orderId int NULL,
    Color varchar(30),
	commodity varchar(50),
	Area varchar(30),
	updatetime timestamp default (now() - interval '1 day')
)
Distributed by (orderId);
/*触发器*/
create or replace function update_timestamp_Commodity_Color_Aging_Model() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_Commodity_Color_Aging_Model on ods_Commodity_Color_Aging_Model;
create trigger auto_updatetime_Commodity_Color_Aging_Model
	before update on ods_Commodity_Color_Aging_Model
	for each row execute procedure update_timestamp_Commodity_Color_Aging_Model();

/*******************************************************cj_Test_mast********************************************************/
DROP TABLE IF EXISTS ods_cj_Test_mast;
CREATE TABLE ods_cj_Test_mast(
    id int,
	Itemid int ,
	fTestType varchar(10) ,
	fSta int ,
	fSubSta int ,
	fResult int ,
	fTester varchar(50) ,
	fRemark varchar(100) ,
	fToolID int ,
	FailureSymptom varchar(200) ,
	CodeFail varchar(100) ,
	CodeDesc0 varchar(100) ,
	Code0 varchar(50) ,
	fDisposition int ,
	cDate timestamp ,
	Date1 timestamp ,
	Date2 timestamp ,
	RepAction varchar(30) ,
	Remark1 varchar(200) ,
	owner varchar(50) ,
	RRTest int ,
	NowErrName varchar(200) ,
	NowErrCode varchar(100) ,
	IS_In21 varchar(10) ,
	PutRightWay varchar(200) ,
	lastUser varchar(30) ,
	WX_Action varchar(200) ,
	NG_Reason varchar(200) ,
	FA_Analysis varchar(200) ,
	PCB_VDD numeric(10, 2) ,
	PCB_AVDD numeric(10, 2) ,
	PCB_VCOM numeric(10, 2) ,
	PCB_VGH numeric(10, 2) ,
	PCB_VGL numeric(10, 2) ,
	PCB_VLED numeric(10, 2) ,
	ECO_VCC numeric(10, 2) ,
    updatetime timestamp default (now() - interval '1 day')
)Distributed by (id);

/*触发器*/
create or replace function update_timestamp_cj_Test_mast() returns trigger as
$$
DECLARE DATE TIMESTAMP;
begin
    SELECT now() - interval '1 day' INTO DATE;
    new.updatetime = DATE;
    return new;
end
$$
language plpgsql;

DROP TRIGGER IF EXISTS auto_updatetime_cj_Test_mast on ods_cj_Test_mast;
create trigger auto_updatetime_cj_Test_mast
	before update on ods_cj_Test_mast
	for each row execute procedure update_timestamp_cj_Test_mast();

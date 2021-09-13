/****************************************************业务维度表（全量）****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_servicecode;
CREATE TABLE dwd_dim_servicecode(
    ServiceCode int,
    Name varchar(200) ,
    code varchar(10),
	ServiceParent int ,
	ServiceCodeName varchar(100) ,
    TestType varchar(10),
    IsDellBusiness boolean,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ServiceCode);

/*注释*/
COMMENT on table dwd_dim_servicecode IS '业务维度表';
COMMENT on column dwd_dim_servicecode.ServiceCode IS '业务ID';
COMMENT on column dwd_dim_servicecode.Name IS '业务名称(英文)';
COMMENT on column dwd_dim_servicecode.ServiceCodeName IS '业务名称（中文）';
COMMENT on column dwd_dim_servicecode.code IS '业务类别ID';
/*COMMENT on column dwd_dim_servicecode.XXXX IS '业务类别名称';*/
COMMENT on column dwd_dim_servicecode.ServiceParent IS '业务大类ID';
/*COMMENT on column dwd_dim_servicecode.XXXX IS '业务大类名称';*/
COMMENT on column dwd_dim_servicecode.TestType IS '工站类别ID(内部开发)';
/*COMMENT on column dwd_dim_servicecode.XXXX IS '工站名称';*/
COMMENT on column dwd_dim_servicecode.IsDellBusiness IS '是否DELL业务';

/*触发器*/
create or replace function update_timestamp_service() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_service
	before update on dwd_dim_servicecode
	for each row execute procedure update_timestamp_service();

/*插入数据*/
INSERT INTO dwd_dim_servicecode (servicecode, name, code, serviceparent, servicecodename, testtype, isdellbusiness)
    SELECT servicecode,name,code,serviceparent,servicecodename,testtype,isdellbusiness
    FROM ods_base_service;

/****************************************************时间维度表（特殊）****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_date_info;
CREATE TABLE dwd_dim_date_info(
    ID  int  NOT NULL,
	fyy varchar(10) ,
	fym varchar(10) ,
	fqm int ,
	fmm int ,
	fwk int ,
	fsDate varchar(20) primary key ,
	fisHDay boolean,
	fDay int,
	fDesc varchar(30) ,
	fdate timestamp,
	updatetime timestamp default now() - interval '1 day'
);Distributed replicated;

/*注释*/
COMMENT on table dwd_dim_date_info IS '时间维度表';
COMMENT on column dwd_dim_date_info.fyy IS '年份';
COMMENT on column dwd_dim_date_info.fym IS '财年';
COMMENT on column dwd_dim_date_info.fqm IS '季度';
COMMENT on column dwd_dim_date_info.fmm IS '月份';
COMMENT on column dwd_dim_date_info.fwk IS '周';
COMMENT on column dwd_dim_date_info.fsDate IS '日期';
COMMENT on column dwd_dim_date_info.fisHDay IS '是否节假日';
COMMENT on column dwd_dim_date_info.fDay IS '星期几';
COMMENT on column dwd_dim_date_info.fDesc IS '详细描述';
COMMENT on column dwd_dim_date_info.fdate IS '详细时间戳';

/*触发器*/
create or replace function update_timestamp_dateinfo() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_dateinfo
	before update on dwd_dim_date_info
	for each row execute procedure update_timestamp_dateinfo();

/*插入数据*/
INSERT INTO dwd_dim_date_info (ID, fyy, fym, fqm, fmm, fwk, fsDate, fisHDay, fDay, fDesc, fdate)
    SELECT id, fyy, fym, fqm, fmm, fwk, fsdate, fishday, fday, fdesc, fdate
    FROM ods_holidays;

/**************************************************Returnto维度表(增量)**************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_returnto;
CREATE TABLE dwd_dim_returnto(
    Returnto varchar(30) primary key NOT NULL,
    Commodity varchar(50),
    servicecategory varchar(100),
    updatetime timestamp default now() - interval '1 day'

)Distributed by (Returnto);

/*注释*/
COMMENT on table dwd_dim_returnto IS 'Returnto维度表';
COMMENT on column dwd_dim_returnto.Returnto IS 'ReturntoID';
COMMENT on column dwd_dim_returnto.commodity IS '产品种类';
COMMENT on column dwd_dim_returnto.servicecategory IS '产品种类';
COMMENT on column dwd_dim_returnto.updatetime IS 'ReturntoID';
/*触发器*/
create or replace function update_timestamp_returnto() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_returnto
	before update on dwd_dim_returnto
	for each row execute procedure update_timestamp_returnto();

/*插入数据*/
INSERT INTO dwd_dim_returnto (Returnto,Commodity,servicecategory)
    SELECT returnto,commodity,servicecategory
    FROM ods_wx_returnto_base;

/*************************************************OEM Location维度表(增量)***********************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_OEM_Location;
CREATE TABLE dwd_dim_OEM_Location(
    OEM_Location varchar(30) primary key NOT NULL,
    FVenName varchar(100),
    Oversea_Local varchar(20),
    CCC_Vendor varchar(20),
    Commodity varchar(50),
    ARM_Request_Type varchar(20),
    request_frequency varchar(50),
    updatetime timestamp default now() - interval '1 day'
)Distributed by (OEM_Location);

/*注释*/
COMMENT on table dwd_dim_OEM_Location IS 'OEM Location维度表';
COMMENT on column dwd_dim_OEM_Location.OEM_Location IS 'OEM ID';
COMMENT on column dwd_dim_OEM_Location.FVenName IS 'returnto站点名称';
COMMENT on column dwd_dim_OEM_Location.Oversea_Local IS '国内or国外';
COMMENT on column dwd_dim_OEM_Location.CCC_Vendor IS 'CCC';
COMMENT on column dwd_dim_OEM_Location.Commodity IS '货件类型';
COMMENT on column dwd_dim_OEM_Location.ARM_Request_Type IS '';
COMMENT on column dwd_dim_OEM_Location.request_frequency IS '要求发货时间段';

/*触发器*/
create or replace function update_timestamp_OEM_Location() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_OEM_Location
	before update on dwd_dim_OEM_Location
	for each row execute procedure update_timestamp_OEM_Location();

/*插入数据*/
INSERT INTO dwd_dim_oem_location (oem_location, fvenname, oversea_local,CCC_Vendor,Commodity,ARM_Request_Type,request_frequency)
    SELECT freturnto as oem_location,fvenname,oversea_local,CCC_Vendor,commodity as Commodity,arm_request_type as ARM_Request_Type,request_frequency
    FROM ods_cj_vendor;

/*****************************************************Rma维度表(增量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_rma;
CREATE TABLE dwd_dim_rma(
    rma varchar(100) primary key NOT NULL,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (rma);

/*注释*/
COMMENT on table dwd_dim_rma IS 'Rma维度表';
COMMENT on column dwd_dim_rma.rma IS 'rma号';

create or replace function update_timestamp_rma() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

/*触发器*/
create trigger auto_update_time_rma
	before update on dwd_dim_rma
	for each row execute procedure update_timestamp_rma();

/*插入数据*/
INSERT INTO dwd_dim_rma (rma)
    SELECT frma as rma
    FROM ods_cj_rmaitem
    group by frma having count(frma) >= 1;

/***************************************************Control维度表(增量)**************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_dim_control;
CREATE TABLE dwd_dim_control(
    fcon varchar(100),
    fppid varchar(100),
    updatetime timestamp default now() - interval '1 day'
)Distributed by (fcon);

/*注释*/
COMMENT on table dwd_dim_control IS 'Control维度表';
COMMENT on column dwd_dim_control.fcon IS '货物ID';
COMMENT on column dwd_dim_control.fppid IS '条码编号';
/*触发器*/
create or replace function update_timestamp_Control() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;
create trigger auto_update_time_control
	before update on dwd_dim_control
	for each row execute procedure update_timestamp_Control();
/*插入数据*/
INSERT INTO dwd_dim_control(fcon, fppid)
    SELECT fcon,fppid
    FROM ods_cj_rmaitem
    group by fcon,fppid having count(fcon) >= 1;
/*************************************************产品处理类型维度表(增量)*************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Rescode;
CREATE TABLE dwd_dim_Rescode(
    FCode int primary key NOT NULL,
	FCodeName varchar(100),
    updatetime timestamp default now() - interval '1 day'
)Distributed by (FCode);

/*注释*/
COMMENT on table dwd_dim_Rescode IS '产品处理类型维度表';
COMMENT on column dwd_dim_Rescode.FCode IS '产品处理类型代码';
COMMENT on column dwd_dim_Rescode.FCodeName IS '产品处理类型名称';

/*触发器*/
create or replace function update_timestamp_Rescode() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_Rescode
	before update on dwd_dim_Rescode
	for each row execute procedure update_timestamp_Rescode();

/*插入数据*/
INSERT INTO dwd_dim_Rescode (fcode, fcodename)
    SELECT fcode,fcodename
    FROM ods_cj_code
    WHERE ftype=9 or ftype=7;

/******************************************************PN维度表(增量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_PN;
CREATE TABLE dwd_dim_PN(
    Pn varchar(20) primary key NOT NULL,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (Pn);

/*注释*/
COMMENT on table dwd_dim_PN IS 'PN维度表';
COMMENT on column dwd_dim_PN.Pn IS 'Pn号';

/*触发器*/
create or replace function update_timestamp_PN() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_PN
	before update on dwd_dim_PN
	for each row execute procedure update_timestamp_PN();

/*插入数据*/
INSERT INTO dwd_dim_PN (Pn)
    SELECT fpn as Pn
    FROM ods_cj_rmaitem
    group by fpn having count(fpn) >= 1;

/******************************************************SN维度表(增量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_SN;
CREATE TABLE dwd_dim_SN(
    Sn varchar(100) primary key NOT NULL,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (Sn);

/*注释*/
COMMENT on table dwd_dim_SN IS 'SN维度表';
COMMENT on column dwd_dim_SN.Sn IS 'Sn号';

/*触发器*/
create or replace function update_timestamp_SN() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_SN
	before update on dwd_dim_SN
	for each row execute procedure update_timestamp_SN();

/*插入数据*/
INSERT INTO dwd_dim_SN (Sn)
    SELECT DISTINCT fsn
    FROM ods_cj_rmaitem
    group by fsn having count(fsn) >= 1;
/******************************************************客户维度表(增量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Custome;
CREATE TABLE dwd_dim_Custome(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    Customercode varchar(100),
    Customer varchar(100) ,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Custome IS '客户维度表';
COMMENT on column dwd_dim_Custome.ID IS '客户ID';
COMMENT on column dwd_dim_Custome.Customer IS '客户大类';
COMMENT on column dwd_dim_Custome.Customercode IS '客户代码';

/*触发器*/
create or replace function update_timestamp_Costome() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_Custome
	before update on dwd_dim_Custome
	for each row execute procedure update_timestamp_Costome();

/*插入数据*/
INSERT INTO dwd_dim_Custome (Customercode,Customer)
SELECT CustomerCode,customer FROM ods_wx_returnto_base
GROUP BY CustomerCode, customer HAVING count(customercode)>=1;


/******************************************************订单状态类型维度表(全量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Order_Orderstate;
CREATE TABLE dwd_dim_Order_Orderstate(
    ID int primary key NOT NULL,
    OrderstateCode varchar(20),
    Orderstatename varchar(100) ,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Order_Orderstate IS '订单状态类型维度表';
COMMENT on column dwd_dim_Order_Orderstate.ID IS '订单状态类型ID';
COMMENT on column dwd_dim_Order_Orderstate.OrderstateCode IS '订单状态类型编码';
COMMENT on column dwd_dim_Order_Orderstate.OrderstateCode IS '订单状态类型名称';

/*触发器*/
create or replace function update_timestamp_orderstate() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_orderstate
	before update on dwd_dim_Order_Orderstate
	for each row execute procedure update_timestamp_orderstate();

/*插入数据*/
INSERT INTO dwd_dim_Order_Orderstate (ID, OrderstateCode, Orderstatename)
    SELECT fcode as ID,fcodename as OrderstateCode,ftypename as Orderstatename
    FROM ods_cj_code
    WHERE ftype=6;
/******************************************************货品状态类型维度表(全量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Goodstate;
CREATE TABLE dwd_dim_Goodstate(
    ID int primary key NOT NULL,
    Goodstatename varchar(100) ,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Goodstate IS '订单状态类型维度表';
COMMENT on column dwd_dim_Goodstate.ID IS '订单状态类型ID';
COMMENT on column dwd_dim_Goodstate.Goodstatename IS '订单状态类型名称';

/*触发器*/
create or replace function update_timestamp_Goodstate() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_Goodstate
	before update on dwd_dim_Goodstate
	for each row execute procedure update_timestamp_Goodstate();

/*插入数据*/
INSERT INTO dwd_dim_Goodstate (ID, Goodstatename)
    SELECT fcode as ID,fcodename as Goodstatename
    FROM ods_cj_code
    WHERE ftype=7 AND ftypename='货品状态';

/******************************************************维修业务状态维度表(全量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Fixstate;
CREATE TABLE dwd_dim_Fixstate(
    ID int primary key NOT NULL,
    Fixstatename varchar(100) ,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Fixstate IS '维修业务状态维度表';
COMMENT on column dwd_dim_Fixstate.ID IS '维修业务状态类型ID';
COMMENT on column dwd_dim_Fixstate.Fixstatename IS '维修业务状态类型名称';

/*触发器*/
create or replace function update_timestamp_fixstate() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_fixstate
	before update on dwd_dim_fixstate
	for each row execute procedure update_timestamp_fixstate();

/*插入数据*/
INSERT INTO dwd_dim_Fixstate (ID, Fixstatename)
    SELECT fcode as ID,fcodename as Fixstatename
    FROM ods_cj_code
    WHERE ftype=7 AND ftypename='维修业务状态';
/******************************************************货物处理类型维度表(全量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Processingtype;
CREATE TABLE dwd_dim_Processingtype(
    ID int primary key NOT NULL,
    Processingtypename varchar(100) ,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Processingtype IS '货物处理类型维度表';
COMMENT on column dwd_dim_Processingtype.ID IS '货件处理类型ID';
COMMENT on column dwd_dim_Processingtype.Processingtypename IS '货件处理类型名称';

/*触发器*/
create or replace function update_timestamp_processingtype() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_processingtype
	before update on dwd_dim_processingtype
	for each row execute procedure update_timestamp_processingtype();

/*插入数据*/
INSERT INTO dwd_dim_Processingtype (ID, Processingtypename)
    SELECT fcode,fcodename
    FROM ods_cj_code
    WHERE ftype=9 AND ftypename='处理类型';
/******************************************************货物维修处理类型维度表(全量)****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Fix_Processingtype;
CREATE TABLE dwd_dim_Fix_Processingtype(
    ID int primary key NOT NULL,
    Fix_Processingtypename varchar(100) ,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Fix_Processingtype IS '货物维修处理类型维度表';
COMMENT on column dwd_dim_Fix_Processingtype.ID IS '货件维修处理类型ID';
COMMENT on column dwd_dim_Fix_Processingtype.Fix_Processingtypename IS '货件维修处理类型名称';

/*触发器*/
create or replace function update_timestamp_Fix_Processingtype() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_Fix_Processingtype
	before update on dwd_dim_Fix_Processingtype
	for each row execute procedure update_timestamp_Fix_Processingtype();

/*插入数据*/
INSERT INTO dwd_dim_Fix_Processingtype (ID, Fix_Processingtypename)
    SELECT fcode as ID,fcodename as Fix_Processingtypename
    FROM ods_cj_code
    WHERE ftype=9 AND ftypename = '维修处理类型' OR ftypename = '维修处理类型-VFIR专用';

/******************************************************地区维度表（全量）****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Area;
CREATE TABLE dwd_dim_Area(
    ID int primary key generated always as identity (START WITH 1 INCREMENT BY 1) NOT NULL,
    Area varchar(100),
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Area IS '地区维度表';
COMMENT on column dwd_dim_Area.ID IS '地区ID';
COMMENT on column dwd_dim_Area.Area IS '地区名';

/*触发器*/
create or replace function update_timestamp_area() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_area
	before update on dwd_dim_area
	for each row execute procedure update_timestamp_area();

/*插入数据*/
INSERT INTO dwd_dim_Area (Area)
SELECT DISTINCT Area FROM
(SELECT CASE WHEN aa.customercode = 'DELL ARB' THEN 'ARB' ELSE 'XM' END AS Area FROM ods_wx_returnto_base aa
GROUP BY aa.customercode HAVING COUNT(aa.customercode)>=1)AS temp;


/******************************************************测试结果维度表（全量）****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Test_Results;
CREATE TABLE dwd_dim_Test_Results(
    ID int primary key generated always as identity (START WITH 1 INCREMENT BY 1) NOT NULL,
    Test_result varchar(100),
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Test_Results IS '测试结果维度表';
COMMENT on column dwd_dim_Test_Results.ID IS '测试结果ID';
COMMENT on column dwd_dim_Test_Results.Test_result IS '测试结果';

/*触发器*/
create or replace function update_timestamp_testresults() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_testresults
	before update on dwd_dim_test_results
	for each row execute procedure update_timestamp_testresults();

/*插入数据*/
INSERT INTO dwd_dim_Test_Results (Test_result)
SELECT FCodeName as Test_result FROM ods_cj_code WHERE fparent='7'AND ftype=22;

/******************************************************不良原因维度表（全量）****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_NG;
CREATE TABLE dwd_dim_NG(
    ID int primary key generated always as identity (START WITH 1 INCREMENT BY 1) NOT NULL,
    NG_Reasen varchar(100),
    NG_Desc varchar(100),
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_NG IS '故障原因维度表';
COMMENT on column dwd_dim_NG.ID IS '故障原因ID';
COMMENT on column dwd_dim_NG.NG_Reasen IS '故障原因';
COMMENT on column dwd_dim_NG.NG_Desc IS '故障类型';

/*触发器*/
create or replace function update_timestamp_ng() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_ng
	before update on dwd_dim_ng
	for each row execute procedure update_timestamp_ng();

/*插入数据*/
INSERT INTO dwd_dim_NG (NG_Reasen, NG_Desc)
SELECT fErrDesc_En as NG_Reasen,fFailReasen_En as NG_Desc FROM ods_cj_odditem
group by fErrDesc_En, fFailReasen_En having count(ferrdesc_en)>=1;

/******************************************************Delay原因维度表(全量）****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_Delay;
CREATE TABLE dwd_dim_Delay(
    ID int primary key generated always as identity (START WITH 1 INCREMENT BY 1) NOT NULL,
    Delay_Type varchar(200),
    Delay_Reason varchar(200),
    Delay_Category varchar(200),
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_Delay IS 'Delay原因维度表';
COMMENT on column dwd_dim_Delay.ID IS 'Delay原因ID';
COMMENT on column dwd_dim_Delay.Delay_Type IS 'Delay类型';
COMMENT on column dwd_dim_Delay.Delay_Reason IS 'Delay原因';
COMMENT on column dwd_dim_Delay.Delay_Category IS 'Delay种类';

/*触发器*/
create or replace function update_timestamp_delay() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_delay
	before update on dwd_dim_delay
	for each row execute procedure update_timestamp_delay();

/*插入数据*/
INSERT INTO dwd_dim_Delay (Delay_Type, Delay_Reason, Delay_Category)
SELECT DISTINCT Delay_Type,Delay_Reason,Delay_Category FROM
(SELECT ocdm.fCodeName as Delay_Type,owwm.fWaitReason as Delay_Reason,fRemark as Delay_Category  FROM ods_cj_delay_mast  ocdm
left outer join ods_wx_wait_master owwm on owwm.fwaittype=ocdm.fcode
left outer join ods_cj_odditem oco on owwm.fitemid = oco.fitemid)as tmp

/***********************************************base_pn_docking维度表*************************************************************/
DROP TABLE IF EXISTS dwd_dim_base_pn_docking;
CREATE TABLE dwd_dim_base_pn_docking(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    PN varchar(100),
    OEM varchar(100),
    MSR varchar(3),
    Model varchar(100),
    frmaclass int,
    fjk varchar(100),
    CLASS varchar(100),
    fsta int,
    fdesc varchar(100),
    Date_SafeLunch date,
    Sta_ARB int,
    Date_SafeLunch_ARB date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_base_pn_docking IS 'base_pn_docking维度表';
COMMENT on column dwd_dim_base_pn_docking.ID IS 'Id自增主键';
COMMENT on column dwd_dim_base_pn_docking.PN IS 'PN号';
COMMENT on column dwd_dim_base_pn_docking.OEM IS 'OEM号';
COMMENT on column dwd_dim_base_pn_docking.MSR IS 'MSR号';
COMMENT on column dwd_dim_base_pn_docking.Model IS 'Model号';
COMMENT on column dwd_dim_base_pn_docking.frmaclass IS 'frmaclass号';
COMMENT on column dwd_dim_base_pn_docking.fjk IS 'fjk型号';
COMMENT on column dwd_dim_base_pn_docking.CLASS IS 'Class号';
COMMENT on column dwd_dim_base_pn_docking.fsta IS 'fsta号';
COMMENT on column dwd_dim_base_pn_docking.fdesc IS '描述信息';
COMMENT on column dwd_dim_base_pn_docking.Date_SafeLunch IS 'SafeLunch日期';
COMMENT on column dwd_dim_base_pn_docking.Sta_ARB IS '-1';
COMMENT on column dwd_dim_base_pn_docking.Date_SafeLunch_ARB IS 'SafeLunch_ARB日期';
COMMENT on column dwd_dim_base_pn_docking.updatetime IS '更新时间戳';
/*触发器*/
create or replace function update_timestamp_base_pn_docking() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_base_pn_docking
	before update on dwd_dim_base_pn_docking
	for each row execute procedure update_timestamp_base_pn_docking();

/*插入数据*/
Insert Into dwd_dim_base_pn_docking(PN,OEM,MSR,Model,frmaclass,fjk,CLASS,fsta,fdesc,Date_SafeLunch,Sta_ARB)
    SELECT     fpn AS PN, fOem AS OEM, fmsrver AS MSR, fModel AS Model, SvrCode as frmaclass, Platform AS fjk, Commodity AS Class, fsta, fdesc,
    fDate_Safe_Luanch::date as Date_SafeLunch, -1 as Sta_ARB
    FROM  ods_base_pn_docking

/***********************************************card维度表*************************************************************/
DROP TABLE IF EXISTS dwd_dim_card;
CREATE TABLE dwd_dim_card(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    PN varchar(100),
    OEM varchar(100),
    MSR varchar(3),
    Model varchar(100),
    frmaclass int,
    fjk varchar(100),
    CLASS varchar(100),
    fsta int,
    fdesc varchar(100),
    Date_SafeLunch date,
    Sta_ARB int,
    Date_SafeLunch_ARB date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_card IS 'card维度表';
COMMENT on column dwd_dim_card.ID IS 'Id自增主键';
COMMENT on column dwd_dim_card.PN IS 'PN号';
COMMENT on column dwd_dim_card.OEM IS 'OEM号';
COMMENT on column dwd_dim_card.MSR IS 'MSR号';
COMMENT on column dwd_dim_card.Model IS 'Model号';
COMMENT on column dwd_dim_card.frmaclass IS 'frmaclass号';
COMMENT on column dwd_dim_card.fjk IS 'fjk型号';
COMMENT on column dwd_dim_card.CLASS IS 'Class号';
COMMENT on column dwd_dim_card.fsta IS 'fsta号';
COMMENT on column dwd_dim_card.fdesc IS '描述信息';
COMMENT on column dwd_dim_card.Date_SafeLunch IS 'SafeLunch日期';
COMMENT on column dwd_dim_card.Sta_ARB IS '-1';
COMMENT on column dwd_dim_card.Date_SafeLunch_ARB IS 'SafeLunch_ARB日期';
COMMENT on column dwd_dim_card.updatetime IS '更新时间戳';
/*触发器*/
create or replace function update_timestamp_card() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_card
	before update on dwd_dim_card
	for each row execute procedure update_timestamp_card();

/*插入数据*/
Insert Into dwd_dim_card(PN,OEM,MSR,Model,frmaclass,fjk,CLASS,fsta,fdesc,Date_SafeLunch,Sta_ARB)
    SELECT     fpn AS PN, fOem AS OEM, fver AS MSR, fModel AS Model, frmaclass, cardbus_type AS fjk, fclass AS Class, fsta, fdesc,
    ApproveDate::date  as Date_SafeLunch, -1 as Sta_ARB
    FROM  ods_cj_card_pn

/*******************************************cj_cpu维度表****************************************************************/
DROP TABLE IF EXISTS dwd_dim_cpu;
CREATE TABLE dwd_dim_cpu(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    PN varchar(6),
    OEM varchar(50),
    MSR varchar(3),
    Model varchar(50),
    frmaclass int,
    fjk varchar(50),
    CLASS varchar(50),
    fsta int,
    fdesc varchar(50),
    Date_SafeLunch date,
    Sta_ARB int,
    Date_SafeLunch_ARB date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_cpu IS 'cj_cpu维度表';
COMMENT on column dwd_dim_cpu.ID IS 'Id自增主键';
COMMENT on column dwd_dim_cpu.PN IS 'PN号';
COMMENT on column dwd_dim_cpu.OEM IS 'OEM号';
COMMENT on column dwd_dim_cpu.MSR IS 'MSR号';
COMMENT on column dwd_dim_cpu.Model IS 'Model号';
COMMENT on column dwd_dim_cpu.frmaclass IS 'frmaclass号';
COMMENT on column dwd_dim_cpu.fjk IS 'fjk型号';
COMMENT on column dwd_dim_cpu.CLASS IS 'Class号';
COMMENT on column dwd_dim_cpu.fsta IS 'fsta号';
COMMENT on column dwd_dim_cpu.fdesc IS '描述信息';
COMMENT on column dwd_dim_cpu.Date_SafeLunch IS 'SafeLunch日期';
COMMENT on column dwd_dim_cpu.Sta_ARB IS '-1';
COMMENT on column dwd_dim_cpu.Date_SafeLunch_ARB IS 'SafeLunch_ARB日期';
COMMENT on column dwd_dim_cpu.updatetime IS '更新时间戳';
/*触发器*/
create or replace function update_timestamp_cpu() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_cpu
	before update on dwd_dim_cpu
	for each row execute procedure update_timestamp_cpu();

/*插入数据*/
Insert Into dwd_dim_cpu(PN,OEM,MSR,Model,frmaclass,fjk,CLASS,fsta,fdesc,Date_SafeLunch,Sta_ARB)
    SELECT  fpn AS PN, '' AS OEM, '' AS MSR, fmodel AS Model, frmaclass, hmu AS fjk, fclass AS Class, fsta, fCustDesc AS fdesc,
    fDate__SafeLunch::date as Date_SafeLunch, -1 as Sta_ARB
    FROM  ods_cj_cpu_pn

/*******************************************cj_hdd维度表****************************************************************/
/*建表*/
DROP TABLE IF EXISTS dwd_dim_hdd;
CREATE TABLE dwd_dim_hdd(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    PN varchar(6),
    OEM varchar(50),
    MSR varchar(3),
    Model varchar(50),
    frmaclass int,
    fjk varchar(50),
    CLASS varchar(50),
    fsta int,
    fdesc varchar(50),
    Date_SafeLunch date,
    Sta_ARB int,
    Date_SafeLunch_ARB date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_hdd IS 'cj_hdd维度表';
COMMENT on column dwd_dim_hdd.ID IS 'Id自增主键';
COMMENT on column dwd_dim_hdd.PN IS 'PN号';
COMMENT on column dwd_dim_hdd.OEM IS 'OEM号';
COMMENT on column dwd_dim_hdd.MSR IS 'MSR号';
COMMENT on column dwd_dim_hdd.Model IS 'Model号';
COMMENT on column dwd_dim_hdd.frmaclass IS 'frmaclass号';
COMMENT on column dwd_dim_hdd.fjk IS 'fjk型号';
COMMENT on column dwd_dim_hdd.CLASS IS 'Class号';
COMMENT on column dwd_dim_hdd.fsta IS 'fsta号';
COMMENT on column dwd_dim_hdd.fdesc IS '描述信息';
COMMENT on column dwd_dim_hdd.Date_SafeLunch IS 'SafeLunch日期';
COMMENT on column dwd_dim_hdd.Sta_ARB IS '-1';
COMMENT on column dwd_dim_hdd.Date_SafeLunch_ARB IS 'SafeLunch_ARB日期';
COMMENT on column dwd_dim_hdd.updatetime IS '更新时间戳';
/*触发器*/
create or replace function update_timestamp_hdd() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_hdd
	before update on dwd_dim_hdd
	for each row execute procedure update_timestamp_hdd();

/*插入数据*/
Insert Into dwd_dim_hdd(PN,OEM,MSR,Model,frmaclass,fjk,CLASS,fsta,fdesc,Date_SafeLunch,Sta_ARB)
    SELECT     fpn AS PN, fOem AS OEM, fVer AS MSR, fModel AS Model, frmaclass, fjk AS fjk, fclass AS CLASS, fsta, fdesc,
    approveDate::date as Date_SafeLunch, -1 as Sta_ARB
    FROM         ods_cj_hdd_pn;

/*******************************************mb维度表****************************************************************/
DROP TABLE IF EXISTS dwd_dim_mb;
CREATE TABLE dwd_dim_mb(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    PN varchar(100),
    OEM varchar(100),
    MSR varchar(3),
    Model varchar(100),
    frmaclass int,
    fjk varchar(100),
    CLASS varchar(100),
    fsta int,
    fdesc varchar(100),
    Date_SafeLunch date,
    Sta_ARB int,
    Date_SafeLunch_ARB date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_mb IS 'mb维度表';
COMMENT on column dwd_dim_mb.ID IS 'Id自增主键';
COMMENT on column dwd_dim_mb.PN IS 'PN号';
COMMENT on column dwd_dim_mb.OEM IS 'OEM号';
COMMENT on column dwd_dim_mb.MSR IS 'MSR号';
COMMENT on column dwd_dim_mb.Model IS 'Model号';
COMMENT on column dwd_dim_mb.frmaclass IS 'frmaclass号';
COMMENT on column dwd_dim_mb.fjk IS 'fjk型号';
COMMENT on column dwd_dim_mb.CLASS IS 'Class号';
COMMENT on column dwd_dim_mb.fsta IS 'fsta号';
COMMENT on column dwd_dim_mb.fdesc IS '描述信息';
COMMENT on column dwd_dim_mb.Date_SafeLunch IS 'SafeLunch日期';
COMMENT on column dwd_dim_mb.Sta_ARB IS '-1';
COMMENT on column dwd_dim_mb.Date_SafeLunch_ARB IS 'SafeLunch_ARB日期';
COMMENT on column dwd_dim_mb.updatetime IS '更新时间戳';
/*触发器*/
create or replace function update_timestamp_mb() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_mb
	before update on dwd_dim_mb
	for each row execute procedure update_timestamp_mb();

/*插入数据*/
Insert Into dwd_dim_mb(PN,OEM,MSR,Model,frmaclass,fjk,CLASS,fsta,fdesc,Date_SafeLunch,Sta_ARB,Date_SafeLunch_ARB)
    SELECT  fpn AS PN, fOem AS OEM, fver AS MSR, fModel AS Model, frmaclass, MSR_ARB AS fjk, '' AS Class, fsta, fdesc,
    fDate_Repair_Safe::date  as Date_SafeLunch, Sta_ARB, fDate_Repair_Safe_ARB::date Date_SafeLunch_ARB
    FROM  ods_cj_mb_pn

/***********************************************mem维度表*************************************************************/
DROP TABLE IF EXISTS dwd_dim_mem;
CREATE TABLE dwd_dim_mem(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    MODEL varchar(30),
    PN varchar(5),
    fdesc varchar(50),
    ODM varchar(30),
    Category varchar(50),
    Capacity varchar(30),
    Spec varchar(30),
    IW_Returnto varchar(20),
    IsECC bpchar(1),
    CartonPN varchar(50),
    PackPN varchar(50),
    fCutin timestamp(6),
    Warranty int4,
    fPhasOut timestamp(6),
    Remark varchar(50),
    Product_Status varchar(10),
    Model_Status varchar(100),
    BGAName varchar(50),
    钢网规格 varchar(30),
    颗粒规格 varchar(30),
    SPC3000 varchar(50),
    HMU varchar(50),
    SW varchar(30),
    fsta int4,
    fisClosed bpchar(1),
    RmaClass int4,
    SPDVer varchar(20),
    MSR varchar(10),
    cDate timestamp(6),
    cuser varchar(10),
    ModelBaseSta varchar(10),
    fDate_Repair_Safe date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*触发器*/
create or replace function update_timestamp_mem() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_mem
	before update on dwd_dim_mem
	for each row execute procedure update_timestamp_mem();

/*标量值函数*/

CREATE or replace FUNCTION Fn_GetProductSta(sta int, phaseout timestamp)
RETURNS varchar (10) as
$body$
DECLARE
     sLoc  Varchar (10);
     icutin  int;
     iphaseout int;
BEGIN
  sloc='UnKnowed';
  if sta in (2,6,11,12) then sloc='OnGoing'; end if;
  if sta in (0,1,4,5) then sloc='NEW'; end if;
  if sta in (3,7) then sloc='EOL'; end if;


  if (phaseout > '1899-01-01 00:00:00.000' ) or  (phaseout is not null) then sloc='EOL'; end if;

   RETURN(sLoc);
END
$body$
language plpgsql;


CREATE or replace FUNCTION Fn_Screen_MEM_ModelIsOpen(sModel Varchar(30))
RETURNS int as
$body$
declare
    res int;
BEGIN
    res = 0;
    if exists (
         SELECT  1 foem FROM ods_cj_mem_model  where fmodel != ''/*sModel*/
         and fpn != '' and foem != '' and fdesc != '' and odm != '' and Category != '' and Capacity != ''
         and spec != '' and BGANAME != '' and spc3000 != '' and  hmu != '' and sw != '' and fisEcc in ('Y', 'N')
         ) then res = 1; end if;

    RETURN (res);
END
$body$
language plpgsql;

/*注释*/
COMMENT on table dwd_dim_mem IS 'mem维度表';

/*插入数据*/
Insert Into dwd_dim_mem(MODEL,PN,fdesc,ODM,Category,Capacity,Spec,IW_Returnto,IsECC,CartonPN,PackPN,fCutin,Warranty,fPhasOut,Remark,Product_Status,Model_Status,BGAName,钢网规格,颗粒规格,SPC3000,HMU,SW,fsta,fisClosed,RmaClass,SPDVer,MSR,cDate,cuser,ModelBaseSta,fDate_Repair_Safe)
    SELECT a.fmodel AS MODEL, a.fpn AS PN, a.fdesc, a.ODM, a.Category, a.Capacity, a.Spec,
      a.foem AS IW_Returnto, a.fisEcc AS IsECC, a.CartonPN, a.PackPN, a.fCutin,
      a.Warranty, a.fPhasOut, a.fRemark AS Remark, Fn_GetProductSta(a.fsta,
      a.fPhasOut) AS Product_Status, b.FCodeName AS Model_Status, a.BGAName,
      a.GWGG AS 钢网规格, a.KLGG AS 颗粒规格, a.SPC3000, a.HMU, a.SW, a.fsta,
      a.fisClosed, a.frmaClass AS RmaClass, a.SPDVer, a.fver as MSR, a.cDate, a.cuser, 'OPEN' AS ModelBaseSta,
      /*CASE WHEN Fn_Screen_MEM_ModelIsOpen(fmodel)
      = 0 THEN 'OPEN' ELSE 'CLOSED' END AS ModelBaseSta, */a.fDate_Repair_Safe::date as fDate_Repair_Safe
FROM ods_cj_mem_model a LEFT OUTER JOIN
      ods_cj_code b ON b.FType = 56 AND b.FCode = a.fsta;

/*更新ModelBaseSta这一列*/
update dwd_dim_mem set ModelBaseSta = 'CLOSED' where (MODEL != ''/*sModel*/
         and PN != '' and IW_Returnto != '' and fdesc != '' and odm != '' and Category != '' and Capacity != ''
         and spec != '' and BGANAME != '' and spc3000 != '' and  hmu != '' and sw != '' and IsECC in ('Y', 'N'));


/******************************************************cj_odd维度表****************************************************/

/*建表*/
DROP TABLE IF EXISTS dwd_dim_odd;
CREATE TABLE dwd_dim_odd(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    PN varchar(6),
    OEM varchar(20),
    MSR varchar(3),
    Model varchar(20),
    frmaclass int,
    fjk varchar(50),
    CLASS varchar(20),
    fsta int,
    fdesc varchar(50),
    Date_SafeLunch date,
    Sta_ARB int,
    Date_SafeLunch_ARB date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_odd IS 'cj_odd维度表';
COMMENT on column dwd_dim_odd.ID IS 'Id自增主键';
COMMENT on column dwd_dim_odd.PN IS 'PN号';
COMMENT on column dwd_dim_odd.OEM IS 'OEM号';
COMMENT on column dwd_dim_odd.MSR IS 'MSR号';
COMMENT on column dwd_dim_odd.Model IS 'Model号';
COMMENT on column dwd_dim_odd.frmaclass IS 'frmaclass号';
COMMENT on column dwd_dim_odd.fjk IS 'fjk型号';
COMMENT on column dwd_dim_odd.CLASS IS 'Class号';
COMMENT on column dwd_dim_odd.fsta IS 'fsta号';
COMMENT on column dwd_dim_odd.fdesc IS '描述信息';
COMMENT on column dwd_dim_odd.Date_SafeLunch IS 'SafeLunch日期';
COMMENT on column dwd_dim_odd.Sta_ARB IS '-1';
COMMENT on column dwd_dim_odd.Date_SafeLunch_ARB IS 'SafeLunch_ARB日期';
COMMENT on column dwd_dim_odd.updatetime IS '更新时间戳';
/*触发器*/
create or replace function update_timestamp_odd() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_odd
	before update on dwd_dim_odd
	for each row execute procedure update_timestamp_odd();

/*插入数据*/
Insert Into dwd_dim_odd(PN,OEM,MSR,Model,frmaclass,fjk,CLASS,fsta,fdesc,Date_SafeLunch,Sta_ARB)
    SELECT     fpn AS PN, fOem AS OEM, fVer AS MSR, fModel AS Model, frmaclass, fremark AS fjk, '' AS CLASS, fsta, fdesc,
    fDate_Repair_Safe::date as Date_SafeLunch, -1 as Sta_ARB
    FROM         ods_cj_odd_pn;

/*******************************************repair_model维度表****************************************************************/
DROP TABLE IF EXISTS dwd_dim_repair_model;
CREATE TABLE dwd_dim_repair_model(
    ID INT generated always as identity (START WITH 1 INCREMENT BY 1) primary key NOT NULL,
    PN varchar(100),
    OEM varchar(100),
    MSR varchar(3),
    Model varchar(100),
    frmaclass int,
    fjk varchar(100),
    CLASS varchar(100),
    fsta int,
    fdesc varchar(100),
    Date_SafeLunch date,
    Sta_ARB int,
    Date_SafeLunch_ARB date,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*注释*/
COMMENT on table dwd_dim_repair_model IS 'repair_model维度表';
COMMENT on column dwd_dim_repair_model.ID IS 'Id自增主键';
COMMENT on column dwd_dim_repair_model.PN IS 'PN号';
COMMENT on column dwd_dim_repair_model.OEM IS 'OEM号';
COMMENT on column dwd_dim_repair_model.MSR IS 'MSR号';
COMMENT on column dwd_dim_repair_model.Model IS 'Model号';
COMMENT on column dwd_dim_repair_model.frmaclass IS 'frmaclass号';
COMMENT on column dwd_dim_repair_model.fjk IS 'fjk型号';
COMMENT on column dwd_dim_repair_model.CLASS IS 'Class号';
COMMENT on column dwd_dim_repair_model.fsta IS 'fsta号';
COMMENT on column dwd_dim_repair_model.fdesc IS '描述信息';
COMMENT on column dwd_dim_repair_model.Date_SafeLunch IS 'SafeLunch日期';
COMMENT on column dwd_dim_repair_model.Sta_ARB IS '-1';
COMMENT on column dwd_dim_repair_model.Date_SafeLunch_ARB IS 'SafeLunch_ARB日期';
COMMENT on column dwd_dim_repair_model.updatetime IS '更新时间戳';
/*触发器*/
create or replace function update_timestamp_repair_model() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_repair_model
	before update on dwd_dim_repair_model
	for each row execute procedure update_timestamp_repair_model();

/*插入数据*/
Insert Into dwd_dim_repair_model(PN,OEM,MSR,Model,frmaclass,fjk,CLASS,fsta,fdesc,Date_SafeLunch,Sta_ARB)
    SELECT     fpn AS PN, fOem AS OEM, fmsrver AS MSR, fModel AS Model, frmaclass, Standard_Accessary_Material AS fjk, fclass AS Class, fsta, fdesc,
    fDate_Safe_Luanch::date  as Date_SafeLunch, -1 as Sta_ARB
    FROM  ods_cj_repair_model

/*************************************************Base_commodity_daily维度*************************************************/
drop table if exists dwd_dim_Base_Commodity_Daily;
create table dwd_dim_Base_Commodity_Daily(
    ID int generated always as identity (start with 1 increment  by 1) not null ,
    Area varchar(30),
    Commodity varchar(50),
    fym varchar(10) NULL,
    fqm int NULL,
    fmm int NULL,
    fwm int NULL,
    fdate timestamp,
    updatetime timestamp default now() - interval '1 day'
)Distributed by (ID);

/*触发器*/
create or replace function update_timestamp_Base_Commodity_Daily() returns trigger as
$$
begin
    new.updatetime = now() - interval '1 day';
    return new;
end
$$
language plpgsql;

create trigger auto_update_time_Base_Commodity_Daily
	before update on dwd_dim_Base_Commodity_Daily
	for each row execute procedure update_timestamp_Base_Commodity_Daily();

/*插入数据*/
INSERT INTO dwd_dim_Base_Commodity_Daily(Area, Commodity, fym, fqm, fmm, fwm, fdate)
SELECT aa.Area,aa.commodity, bb.fym, fqm, fmm, fwk as fwm ,fdate
 from ods_Commodity_Color_Aging_Model aa, ods_Holidays bb
 where   bb.fym >= 'FY19'

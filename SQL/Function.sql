/**********************************************************函数*********************************************************/
create or replace function fn_DELL_IsARB_Returnto(sReturnto varchar(30))
returns int
as
$body$
DECLARE
    iRes int;
begin
   iRes = 0;
   if sReturnto in ('ARBCJAIOMB','ARBCJDTMB','ARBCJMON','ARBCJNBMB','ARBCJAIOLCD')  then iRes = 1;
   end if;

   return COALESCE(iRes,0);
end
$body$
language plpgsql;


create or replace function fn_FPD_PN_fmsta(Returnto varchar(30),fmfsta int,fsta_ARB int)
returns int
as
$$
begin
  if fn_DELL_IsARB_Returnto(Returnto)=1 then return COALESCE(fsta_ARB,1);
  end if;
  return COALESCE(fmfsta,1);
end
$$
language plpgsql;

create or replace function fn_FPD_PN_ApproveDate(Returnto varchar(30),ApproveDate timestamp,Date_SafeLunch_ARB timestamp) --返回Monitor的SafeLuanch
returns timestamp
as
$$
begin
  if fn_DELL_IsARB_Returnto(Returnto)=1 then return Date_SafeLunch_ARB;
  end if;
  return ApproveDate;
end
$$
language plpgsql;




create or replace function Fn_ODM_isClosedSta(iSta int)
returns int
as
$$
DECLARE
iRes int;
begin
    iRes = 0;
    if iSta in (-10,19,29,39,59) then iRes = 1;
    end if;
    return(iRes);
end
$$
language plpgsql;


create or replace function cj_KPI_DELL_IsScrap(
sReturnto varchar(200),iSvrCode int, iSta int, iResCode int, iVMIRes int, iRepairRes int, iDisposition varchar(200)
)
returns varchar(200)
as
$$
DECLARE
    sRes varchar(200);
begin
   sRes = '' ;
   if iSvrCode = 13 --FPD
   then
   begin
     if iSta in (19,51,52,53, 59)   --好件
     then
     begin
        sRes = 'No';
     end;
     else
        begin
        if iDisposition <> ''  then  sRes = iDisposition;  --BR、DES
        else  if iSta  in (34,39,49) then  sRes = 'Yes';
        end if;
        end if;
        end;
     end if;
   end;
   else
   begin
     if iSta = 19 --好件
     then
     begin
       sRes = 'No';
     end;
     else
     begin
       if iDisposition <> ''
           then
             sRes = iDisposition;  --BR、DES
       else
       begin
         if iSta = 29
         then
         begin
           if iResCode = 2 then  sRes = 'Yes';end if;
           if iResCode = 3 then  sRes = 'RTV';end if;
         end;
         end if;
       end;
       end if;
      --    else if (@iSta = 29) OR (@iResCode IN (2,3)) SET @sRes = 'Yes'
     end;
     end if;
   end;
   end if;

   return sRes;
end;
$$
language plpgsql;

create or replace function cj_Get_StartDate(iSvrCode int, dReceiveDate timestamp, dSafeLunchDate timestamp, dOtherOnholdDate timestamp)
returns timestamp
as
$$
DECLARE dRes date;
begin

   dRes = null; -- @dReceiveDate

   if dReceiveDate is not null
   then
   begin
     if dSafeLunchDate is null
         then dRes = dReceiveDate;
     else
     begin
       if dReceiveDate < dSafeLunchDate then dRes = dSafeLunchDate ;
       else dRes = dReceiveDate;
       end if;
     end;
     end if;

     if dOtherOnholdDate is not null and dRes is not null
     then
     begin
       if dRes < dOtherOnholdDate then dRes = dOtherOnholdDate;
       end if;
     end;
     end if;
   end;
   end if;
   return dRes;
end
$$
language plpgsql;

--维修结果不能为空，分NFF/FG/Scrap/Internal use/RTV 5种
create or replace function cj_KPI_DELL_FinalResult(
  sReturnto varchar(50),iSvrCode int, iSta int, iResCode int, iVMIRes int, iRepairRes int, iDisposition varchar(10))
returns varchar(20)
as
$$
DECLARE sRes varchar(20);
begin

   sRes = '' ;
   if iSvrCode = 13 --FPD
   then
   begin
     if iSta in (19,51,52,53, 59)   --好件
     then
     begin
       if iDisposition = 'CND'
           then sRes = 'NFF';
       else sRes = 'FG';
       end if;
     end;
     else
     begin
       if iSta  in (34,39) then sRes = 'Scrap';end if;
       if iSta = 49 then sRes = 'Internal use';end if;
     end;
     end if;
   end;
   else
   begin
     if iSta = 19 --好件
     then
     begin
       if iSvrCode in (21,22,36)
           then sRes = 'NFF';  --CPU, HDD, CARD 测试项目好件为NFF
       else
         if iDisposition = 'CND'
             then  sRes = 'NFF';
         else  sRes = 'FG';
         end if;
       end if;
     end;
     else
     begin
       if iSvrCode in (205,218)  --DELL AIO LCD
       then
       begin
         if iRepairRes = 2
             then sRes = 'Scrap';
   --      if @iRepairRes = 3 set @sRes = 'RTV'
         end if;
       end;
       else
       begin
         sRes = case when iResCode = 2 then 'Scrap'
                          when iResCode = 3 then 'RTV'
                          when iSta = 29 and iSvrCode = 22 then 'RTV'
                     end;

       end;
       end if;
     end;
     end if;
   end ;
   end if;
   return sRes;
end
$$
language plpgsql;

create or replace function public.fn_workday(date1 timestamp, date2 timestamp) returns int4
as
$$
DECLARE Day1 int;
        Day2 int;
        Day int;
BEGIN


       Day1= date_part('day', date2::timestamp-date1::timestamp);
       Day2 = (SELECT COUNT(id)
                FROM public.dwd_dim_date_info ho
                 WHERE fisHDay = TRUE AND ho.fdate between  DATE1 AND DATE2);
       Day=Day1-Day2;
       RETURN (Day);

end
$$
language plpgsql;


drop function IF EXISTS Dell_KPI_days(tableName text);
create or replace function Dell_KPI_days(tableName text)
returns text as
$$
declare
    ARB_YN text;
    CJINUSE text;
    Return_CJINUSE text;
    Final_result text;
    Fack_PN text;
    TAT_close text;
    TAT_pack text;
    TAT_sys text;
    mycode text;
    IS_90RR text;
    Last_date text;
    error int;

begin

ARB_YN:='/****************************************************ARB_YN****************************************************/
update ' ||quote_ident(tableName)||
       ' set ARB_YN = case when substring(RequestRef, 1,3) = ''ARB'' then ''Y'' else ''N'' end
    where ARB_YN is null and updatetime::date=(now() - interval ''1 day'')::date;';

CJINUSE:='/****************************************************FPD 排除CJINUSE****************************************************/
delete from ' ||quote_ident(tableName) ||
       ' where svrcode = 13
    and return_rma = ''CJINUSE''
    and SUBSTRING(Con, 1,4) =''CJIN'';';
Return_CJINUSE:='/******************************************FPD 排除CJINUSE 且上一次Returnto 为 CJSPHL*************************************/
delete from ' ||quote_ident(tableName)||
       ' where Area in (''XM'',''ARB'') and return_rma = ''CJINUSE'' and Returnto_Last = ''CJSPHL'';
update ' ||quote_ident(tableName)||
       ' set Start_date = cj_Get_StartDate(SvrCode, Receive_Date, Date_safelunch, null )
    where Area in (''XM'',''ARB'') and Start_date is null and updatetime::date=(now() - interval ''1 day'')::date;';
Final_result:='/*********************************************************维修结果*******************************************************/
update ' ||quote_ident(tableName) ||
       '  set Final_Result =cj_KPI_DELL_FinalResult(Returnto, SvrCode, fsta, frescode, fVIMFunRes, fRepairRes, Test_Result)
    where Area in (''XM'', ''ARB'') and COALESCE(Final_Result, '''') <>
          cj_KPI_DELL_FinalResult(Returnto, SvrCode, fsta, frescode, fVIMFunRes, fRepairRes, Test_Result)
    and
    SvrCode <> 70 and updatetime::date=(now() - interval ''1 day'')::date;';
Fack_PN:='/***********************************************************Fack_PN*********************************************************/
update ' ||quote_ident(tableName)||
       ' set IsFakePart =
      case when POSITION(''Fake Part'' IN ErrDesc_VFIR)>0 then ''Y''
           when POSITION(''Fake PN'' IN ErrDesc_VFIR)>0  THEN ''Y''
           when POSITION(''假'' IN NG_ErrorDesc)>0  THEN ''Y''
           when POSITION(''Fake Part'' IN NG_ErrorDesc)>0  THEN ''Y''
           when POSITION(''Fake PN'' IN NG_ErrorDesc)>0  THEN ''Y''
       else ''N'' end
  WHERE COALESCE(IsFakePart , '''') = '''' and Area in (''ARB'', ''XM'') and updatetime::date=(now() - interval ''1 day'')::date;';
TAT_close:='/*******************************************************TAT_close*******************************************************/
DROP TABLE IF EXISTS temp_in;
CREATE TEMPORARY TABLE temp_in(id INT,TAT INT);
INSERT INTO temp_in(ID, TAT)
SELECT ID,TAT FROM
(
SELECT dfcbo.ID AS ID,COUNT(ho.id) AS TAT
FROM ods_holidays ho,'||quote_ident(tableName)||' dfcbo
WHERE (
  dfcbo.updatetime::date=(now() - interval ''1 day'')::date
  and ho.fdate between dfcbo.start_date and dfcbo.close_date
  and ho.fishday = FALSE
  and Final_Result in (''FG'', ''NFF'')
  and dfcbo.Area in (''ARB'', ''XM'')
  and COALESCE(dfcbo.TAT_Closed, -1) = -1
  and dfcbo.Close_Date is not null
    )
GROUP BY dfcbo.ID
) AS TMP;

DROP TABLE IF EXISTS temp_NOTin;
CREATE TEMPORARY TABLE temp_NOTin(id INT,TAT INT);
INSERT INTO temp_NOTin(ID, TAT)
SELECT ID,TAT FROM
(
SELECT dfcbo.ID AS ID,COUNT(ho.id) AS TAT
FROM ods_holidays ho,'||quote_ident(tableName)||' dfcbo
WHERE (
  dfcbo.updatetime::date=(now() - interval ''1 day'')::date
  and ho.fdate between dfcbo.start_date and COALESCE(dfcbo.Date_Bad_Post, dfcbo.Close_Date)
  and ho.fishday = FALSE
  and Final_Result NOT IN (''FG'', ''NFF'')
  and dfcbo.Area in (''ARB'', ''XM'')
  and COALESCE(dfcbo.TAT_Closed, -1) = -1
  and dfcbo.Close_Date is not null
    )
GROUP BY dfcbo.ID
) AS TMP2;
select * from temp_NOTin;


update '||quote_ident(tableName)||' dfCbOD set (ID,TAT_Closed) =(
		SELECT I.id,TAT FROM temp_in I WHERE dfCbOD.ID = I.ID)
		where EXISTS(SELECT temp_in.id FROM temp_in where temp_in.id=dfCbOD.id)
           and updatetime::date=(now() - interval ''1 day'')::date;

update '||quote_ident(tableName)||' dfCbOD set (ID,TAT_Closed) =(
		SELECT NI.id,TAT FROM temp_NOTin NI
		    WHERE dfCbOD.ID = NI.ID)
		where EXISTS(SELECT temp_NOTin.id FROM temp_NOTin where temp_NOTin.id=dfCbOD.id)
		  and updatetime::date=(now() - interval ''1 day'')::date;

DROP TABLE IF EXISTS temp_in2;
CREATE TEMPORARY TABLE temp_in2(id INT,TAT INT);
INSERT INTO temp_in2(ID, TAT)
SELECT ID,TAT FROM
(
SELECT dfcbo.ID AS ID,COUNT(ho.id) AS TAT
FROM ods_holidays ho,'||quote_ident(tableName)||' dfcbo
WHERE (
              dfcbo.updatetime::date = (now() - interval ''1 day'')::date
              and ho.fdate between dfcbo.start_date and dfcbo.close_date
              and ho.fishday = FALSE
              and Final_Result IN (''FG'', ''NFF'')
              and dfcbo.Area in (''ARB'', ''XM'')
              and TAT_Closed = 0
              and dfcbo.Close_Date is not null
              and Start_Date is not null
              and Start_Date < Close_Date
          )
GROUP BY dfcbo.ID) AS TMP;
select * from temp_in2;

DROP TABLE IF EXISTS temp_NOTin2;
CREATE TEMPORARY TABLE temp_NOTin2(id INT,TAT INT);
INSERT INTO temp_NOTin2(ID, TAT)
SELECT ID,TAT FROM
(
SELECT dfcbo.ID AS ID,COUNT(ho.id) AS TAT
FROM ods_holidays ho,'||quote_ident(tableName)||' dfcbo
WHERE (
  dfcbo.updatetime::date=(now() - interval ''1 day'')::date
  and ho.fdate between dfcbo.start_date and COALESCE(dfcbo.Date_Bad_Post, dfcbo.Close_Date)
  and ho.fishday = FALSE
  and Final_Result NOT IN (''FG'', ''NFF'')
  and dfcbo.Area in (''ARB'', ''XM'')
  and TAT_Closed = 0
  and dfcbo.Close_Date is not null
  and Start_Date is not null
  and Start_Date < Close_Date
  )
GROUP BY dfcbo.ID) AS TMP2;

update '||quote_ident(tableName)||' dfCbOD set (ID,TAT_Closed) =(
		SELECT I.id,TAT FROM temp_in2 I WHERE dfCbOD.ID = I.ID)
		where
		  EXISTS(SELECT temp_in2.id FROM temp_in2 where temp_in2.id=dfCbOD.id)
		  and updatetime::date=(now() - interval ''1 day'')::date;

update '||quote_ident(tableName)||' dfCbOD set (ID,TAT_Closed) =(
		SELECT NI.id,TAT
		FROM temp_NOTin2 NI
		WHERE dfCbOD.ID = NI.ID)
		where
		  EXISTS(SELECT temp_NOTin2.id FROM temp_NOTin2 where temp_NOTin2.id=dfCbOD.id)
		  and updatetime::date=(now() - interval ''1 day'')::date;

update '||quote_ident(tableName)||' dfCbOD set TAT_Closed = 0 where TAT_Closed < 0 and updatetime::date=(now() - interval ''1 day'')::date;';
TAT_pack:='/*****************************************************TAT_pack**********************************************************/
DROP TABLE IF EXISTS temp_packing;
CREATE TEMPORARY TABLE temp_packing(id INT,TAT INT);
INSERT INTO temp_packing(ID, TAT)
SELECT ID,TAT FROM
(SELECT dfcbo.ID AS ID,COUNT(ho.id) AS TAT
FROM ods_holidays ho,'||quote_ident(tableName)||' dfcbo
WHERE (
              dfcbo.updatetime::date = (now() - interval ''1 day'')::date
              and ho.fdate between dfcbo.start_date and dfcbo.Packing_Date
              and ho.fishday = FALSE
              and Final_Result IN (''FG'', ''NFF'')
              and dfcbo.Area in (''ARB'', ''XM'')
              and COALESCE(dfcbo.TAT_Packing, -1) = -1
              and dfcbo.Packing_Date is not null
          )
GROUP BY dfcbo.ID) AS TMP;

update '||quote_ident(tableName)||' dfCbOD
set  (ID,TAT_Packing) = (
		SELECT PK.id,TAT
		FROM temp_packing PK
		WHERE dfCbOD.ID = PK.ID)
where EXISTS(SELECT temp_packing.id FROM temp_packing where temp_packing.id=dfCbOD.id)
      and updatetime::date=(now() - interval ''1 day'')::date;

DROP TABLE IF EXISTS temp_packing2;
CREATE TEMPORARY TABLE temp_packing2(id INT,TAT INT);
INSERT INTO temp_packing2(ID, TAT)
SELECT ID,TAT FROM
(SELECT dfcbo.ID AS ID,COUNT(ho.id) AS TAT
FROM ods_holidays ho,'||quote_ident(tableName)||' dfcbo
WHERE (
              dfcbo.updatetime::date = (now() - interval ''1 day'')::date
              and ho.fdate between dfcbo.start_date and dfcbo.Packing_Date
              and ho.fishday = FALSE
              and dfcbo.Area in (''ARB'', ''XM'')
              and TAT_Packing = 0
              and dfcbo.Packing_Date is not null
              and Start_Date is not null
              and Start_Date < Packing_Date
          )
GROUP BY dfcbo.ID) AS TMP;

update '||quote_ident(tableName)||' dfCbOD
set  (ID,TAT_Packing) = (
		SELECT PK.id,TAT
		FROM temp_packing2 PK
		WHERE dfCbOD.ID = PK.ID)
where EXISTS(SELECT temp_packing2.id FROM temp_packing2 where temp_packing2.id=dfCbOD.id)
	  and dfCbOD.updatetime::date=(now() - interval ''1 day'')::date;

update '||quote_ident(tableName)||' dfCbOD
set TAT_Packing = 0
where TAT_Packing < 0
      and updatetime::date=(now() - interval ''1 day'')::date;';

TAT_sys:='/*************************************************更新TAT_sys************************************************/
update '||quote_ident(tableName)||'
set TAT_sys=
    case when Area=''XM''
    then TAT_Closed
    else TAT_Packing
    end
  where COALESCE(TAT_sys, -1) < 0
    and updatetime::date=(now() - interval ''1 day'')::date;';


IS_90RR:='/******************************************************IS_90RR*******************************************************/
update ' ||quote_ident(tableName)||
          ' aa set is_90rr =
    case when IsFakePart = ''Y'' then ''N''   --假PN 不计90RR
         when aa.Commodity = ''CPU'' AND aa.Last_ImportantNote = ''RNU'' THEN ''N''
         when aa.Commodity = ''CPU'' AND aa.ImportantNote = ''RNU'' then ''N''
         when last_closed_date IS NOT NULL AND is_90rr(receive_date,last_closed_date)<=90
              and  substring(COALESCE(aa.Last_RequestREF, ''''), 1,3) <> ''ARB''  then ''Y''
         when last_closed_date IS NOT NULL AND is_90rr(receive_date,last_closed_date)<=90
              and  substring(COALESCE(aa.Last_RequestREF, ''''), 1,3) = ''ARB''  then ''Y-ARB''
    else ''N''
    end
where aa.Area in (''ARB'', ''XM'') and aa.LastID > 0
AND  aa.SvrCode <> 70;';
Last_date:='/******************************************************Last_date*******************************************************/
update ' || quote_ident(tableName)||
           ' set (Last_Year_Code, Last_Quarter_Code,
            Last_Month_Code,Last_Week_Code) = (bb.fym,bb.fqm, bb.fmm,bb.fwk)
from ods_holidays bb
where Last_Closed_Date=bb.fdate;';

execute ARB_YN
      ||CJINUSE
      ||Return_CJINUSE
      ||Final_result
      ||Fack_PN
      ||TAT_close
      ||TAT_pack
      ||TAT_sys
      ||IS_90RR
      ||Last_date;

return mycode='success';

end;

$$ language plpgsql;

create or replace function public.is_90RR(date1 timestamp, date2 timestamp)
returns int4
as
$$
DECLARE Day int;
BEGIN
    Day=date_part('day', date2::timestamp-date1::timestamp);
    RETURN (Day);
end
$$
language plpgsql;

create or replace function Get_WIP_Goal(sCommodity varchar(50))
returns int4
AS
$$
DECLARE iRes int;
BEGIN
   iRes = 5; -- 默认5 天
   if sCommodity in ('HDD', 'CPU', 'CARD')
   then iRes = 3; --测试业务 3 天
   end if;
   RETURN(iRes);
end
$$
language plpgsql;

create or replace function Fn_AgingDayPer_KPI (days int)
RETURNS varchar (10)
AS
$$
DECLARE sPer  Varchar (50);
BEGIN
  IF days <=5  then sPer='5days';
  ELSEIF (days >5)  and  (days <=10)  then sPer='6_10days' ;
  ELSEIF (days >10)  and  (days <=19)  then sPer='11_19days';
  ELSEIF (days >19)  and  (days <=29)  then sPer='20_29days';
  ELSEIF (days >29)  and  (days <=60)  then sPer='30_60days';
  ELSEIF days > 60  then sPer='60days';
  END IF;
RETURN(sPer);
END
$$
language plpgsql;

drop function IF EXISTS Dell_KPI_daily_WIP();
create or replace function Dell_KPI_daily_WIP()
returns text as
$$
declare
    update_daily_WIP text;
    mycode text;
BEGIN
    update_daily_WIP:='INSERT INTO topic.dws_Report_DELL_Daily_WIP(area, commodity, qty, days_5, days_more5)
select AREA, Commodity,COUNT(1) as Qty, 0 as Days_5, 0 as Days_More5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
    and RmaSta > 0
    and updatetime::date=(now() - interval ''1 day'')::date
  --  where Receive_Date is not null
    group by AREA, Commodity;

update topic.dws_Report_DELL_Daily_WIP set Days_5 = bb.Days5
    from topic.dws_Report_DELL_Daily_WIP aa
    inner join (
    select AREA, Commodity,updatetime, COUNT(1) as Days5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta > 0
      and AgingDays <= public.Get_WIP_Goal(Commodity)
      and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
      and updatetime::date=(now() - interval ''1 day'')::date
    group by AREA, Commodity,updatetime
    )     bb on aa.area = bb.AREA and bb.Commodity = aa.Commodity and aa.updatetime::date = bb.updatetime::date;

update topic.dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Days_5) =(
	select bb.AREA,bb.Commodity,bb.Days5 from
    (select AREA, Commodity,updatetime, COUNT(1) as Days5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta > 0
      and AgingDays <= public.Get_WIP_Goal(Commodity)
      and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
      and updatetime::date=(now() - interval ''1 day'')::date
    group by AREA, Commodity,updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta > 0
                             and AgingDays <= public.Get_WIP_Goal(Commodity)
                             and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
                             and updatetime::date=(now() - interval ''1 day'')::date
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval ''1 day'')::date;

update topic.dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Days_5) =(
	select bb.AREA,bb.Commodity,aa.days_5 + bb.Days5 from
    (select AREA, Commodity,updatetime, COUNT(1) as Days5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta > 0
      and Receive_Date is null
      and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
    group by AREA, Commodity,updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta > 0
                             and Receive_Date is null
                             and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval ''1 day'')::date;

update topic.dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Days_More5) =(
	select bb.AREA,bb.Commodity,bb.Days_More5 from
    (select AREA, Commodity, updatetime, COUNT(1) as Days_More5
        from public.dwd_fact_report_dell_kpi_item_wip
        where RmaSta >0
          and AgingDays > public.Get_WIP_Goal(Commodity)
          and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
        group by AREA, Commodity,updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta >0
                             and AgingDays > public.Get_WIP_Goal(Commodity)
                             and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval ''1 day'')::date;

update topic.dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Onway) =(
	select bb.AREA,bb.Commodity, bb.OnWay from
    (select AREA, Commodity, updatetime, COUNT(1) as OnWay
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta = 0 and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
    group by AREA, Commodity, updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta = 0 and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
    group by AREA, Commodity, updatetime)
      and updatetime::date=(now() - interval ''1 day'')::date;

update topic.dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,AWP) =(
	select bb.AREA,bb.Commodity,bb.AWP from
    (select AREA, Commodity, updatetime, COUNT(1) as AWP
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta >0
      and "Is_On hold" = ''Y''
      and DelayType = ''Material shortage''
      and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
    group by AREA, Commodity, updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta >0
                             and "Is_On hold" = ''Y''
                             and DelayType = ''Material shortage''
                             and RETURNTO_RMA not in  (''CJSPHL'', ''CJINUSE'')
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval ''1 day'')::date;';
execute update_daily_wip;
mycode='success';
RETURN(mycode);
END
$$
language plpgsql;

drop function IF EXISTS Dell_KPI_daily();
create or replace function Dell_KPI_daily()
returns text as
$$
declare
    update_daily text;
    mycode text;
BEGIN
    update_daily:='/*Received*/
insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate,''Received'' as fType ,COALESCE(bb.qty,0) as QTY
  from public.dwd_dim_base_commodity_daily aa
  inner join
  (
    select Area, Commodity, Receive_Date, COUNT(1) as Qty
    from public.dwd_fact_orderdetail
    where updatetime::date=(now() - interval ''1 day'')::date
    group by Area, Commodity, Receive_Date
  ) bb on aa.area = bb.area and aa.Commodity = bb.Commodity and aa.fdate=bb.Receive_Date
where updatetime::date=(now() - interval ''1 day'')::date;
/*Close*/
insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, bb.fType, COALESCE(bb.Qty,0) as Qty
from public.dwd_dim_base_commodity_daily aa
inner join
  (
    select Area, Commodity, Close_Date, Final_Result as fType,  COUNT(1) as Qty
    from public.dwd_fact_close_base_orderdetail
    where updatetime::date=(now() - interval ''1 day'')::date
    group by Area, Commodity, Close_Date, Final_Result

    union all

    select Area, Commodity, Close_Date, Final_Result as fType,  COUNT(1) as Qty
    from public.dwd_fact_close_monitor_orderdetail
    where updatetime::date=(now() - interval ''1 day'')::date
    group by Area, Commodity, Close_Date, Final_Result

    union all

    select Area, Commodity, Close_Date, Final_Result as fType,  COUNT(1) as Qty
    from public.dwd_fact_close_panel_orderdetail
    where updatetime::date=(now() - interval ''1 day'')::date
    group by Area, Commodity, Close_Date, Final_Result
   ) bb on aa.area= bb.Area and aa.commodity=bb.Commodity and aa.fdate=bb.Close_Date
   where updatetime::date=(now() - interval ''1 day'')::date;
/*Closed  Average TAT<=5wd*/
    insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
    select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, bb.fType,COALESCE(bb.Qty,0) as Qty
    from public.dwd_dim_base_commodity_daily aa
    inner join
    (
      select Area, Commodity, Close_Date, ''Average TAT<=5wd'' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_base_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity)
        and Final_Result in (''NFF'', ''FG'')
        and updatetime::date=(now() - interval ''1 day'')::date
      group by Area, Commodity, Close_Date
      union all
    select Area, Commodity, Close_Date, ''Average TAT<=5wd'' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_monitor_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity)
        and Final_Result in (''NFF'', ''FG'')
        and updatetime::date=(now() - interval ''1 day'')::date
      group by Area, Commodity, Close_Date
      union all
     select Area, Commodity, Close_Date, ''Average TAT<=5wd'' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_panel_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity)
        and Final_Result in (''NFF'', ''FG'')
        and updatetime::date=(now() - interval ''1 day'')::date
      group by Area, Commodity, Close_Date
     )  bb on aa.area= bb.Area and aa.commodity=bb.Commodity and aa.fdate=bb.Close_Date
      where updatetime::date=(now() - interval ''1 day'')::date;

/*Closed  Average TAT Total<=5wd*/
    insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
    select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, bb.fType,COALESCE(bb.Qty,0) as Qty
    from public.dwd_dim_base_commodity_daily aa
    inner join
    (
      select Area, Commodity, Close_Date, ''Average TAT Total<=5wd'' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_base_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity) -- and Final_Result in (''NFF'', ''FG'')
      and updatetime::date=(now() - interval ''1 day'')::date
      group by Area, Commodity, Close_Date
      union all
      select Area, Commodity, Close_Date, ''Average TAT Total<=5wd'' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_monitor_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity) -- and Final_Result in (''NFF'', ''FG'')
      and updatetime::date=(now() - interval ''1 day'')::date
      group by Area, Commodity, Close_Date
      union all
      select Area, Commodity, Close_Date, ''Average TAT Total<=5wd'' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_panel_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity) -- and Final_Result in (''NFF'', ''FG'')
      and updatetime::date=(now() - interval ''1 day'')::date
      group by Area, Commodity, Close_Date
     )  bb on aa.area= bb.Area and aa.commodity=bb.Commodity and aa.fdate=bb.Close_Date
      where updatetime::date=(now() - interval ''1 day'')::date;

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, ''WIP'' as fType ,COALESCE(bb.qty,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join topic.dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
       and aa.commodity = bb.Commodity
       and aa.fdate::date= bb.updatetime::date
       and bb.Qty > 0
   where aa.updatetime::date=(now() - interval ''1 day'')::date;

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, ''OnWay'' as fType ,COALESCE(bb.OnWay,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join topic.dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
       and aa.commodity = bb.Commodity
       and aa.fdate::date= bb.updatetime::date
       and bb.onWay > 0
   where aa.updatetime::date=(now() - interval ''1 day'')::date;

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, ''WIP<=5WD'' as fType ,COALESCE(bb.Days_5,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join topic.dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
       and  aa.commodity = bb.Commodity
       and aa.fdate::date= bb.updatetime::date
       and COALESCE(bb.Days_5, 0) > 0
   where aa.updatetime::date=(now() - interval ''1 day'')::date;

insert into  topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, ''WIP>5WD'' as fType ,COALESCE(bb.Days_More5,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join topic.dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
              and  aa.commodity = bb.Commodity
              --and aa.fdate= bb.updatetime
   and COALESCE(bb.Days_More5, 0) > 0
   where aa.updatetime::date=(now() - interval ''1 day'')::date;


insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, ''AWP'' as fType ,COALESCE(bb.AWP,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join topic.dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
              and  aa.commodity = bb.Commodity
              and aa.fdate::date=bb.updatetime::date
   and COALESCE(bb.AWP, 0) > 0
   where aa.updatetime::date=(now() - interval ''1 day'')::date;

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区,aa.种类,aa.财年,aa.季度,aa.月度,aa.周,aa.日期,''Total'' as 统计类型,  SUM(aa.数量) as 数量
   from topic.dws_每日统计信息 aa
   where 种类 in  (''MEM'',''CPU'', ''HDD'', ''CARD'')
   and 统计类型 in (''NFF'', ''FG'', ''Scrap'', ''RTV'')
   and  aa.updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期;

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区,aa.种类,aa.财年,aa.季度,aa.月度,aa.周,aa.日期, ''Total'' as 统计类型,  SUM(aa.数量) as 数量
   from topic.dws_每日统计信息 aa
   where  种类 not in  (''MEM'',''CPU'', ''HDD'', ''CARD'')
   and 统计类型 in (''NFF'', ''FG'', ''Scrap'', ''Internal use'' )
   and  aa.updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期;

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区,aa.种类,aa.财年,aa.季度,aa.月度,aa.周,aa.日期, aa.统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 季度, 月度, 周, 日期, ''AverageTAT<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' ) and  updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期
   ) aa
   left outer join topic.dws_每日统计信息 bb on aa.地区 = bb.地区 and aa.种类 = bb.种类
   and aa.财年= bb.财年 and aa.日期 = bb.日期 and bb.统计类型 = ''Average TAT<=5wd'';

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期, aa.统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/ aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 季度, 月度, 周, 日期, ''AverageTAT Total<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where  统计类型 = ''Total'' and updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期
   ) aa
   left outer join topic.dws_每日统计信息 bb on aa.地区 = bb.地区 and aa.种类 = bb.种类
   and aa.财年 = bb.财年 and aa.日期 = bb.日期 and bb.统计类型 = ''Average TAT Total<=5wd'';

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期, ''Planning Yield (Goal:65%)'' as 统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/ aa.数量) else 0 end as 数量
   from topic.dws_每日统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 季度, 月度, 周, 日期, SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期
   )  bb ON aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.财年 = bb.财年 and aa.日期 = bb.日期
 --  left outer join DELL_KPI_Daily_item cc
  -- ON cc.fType = ''RTV'' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym AND AA.fDate = CC.fDate
   where aa.统计类型 = ''Total''   and  updatetime::date=(now() - interval ''1 day'')::date;

insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期, ''NFF rate'' as 统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/ aa.数量) else 0 end as 数量
   from topic.dws_每日统计信息 aa
   LEFT OUTER JOIN topic.dws_每日统计信息 bb
   ON aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.财年 = bb.财年 and aa.日期 = bb.日期
   and bb.统计类型 = ''NFF''
   where aa.统计类型 = ''Total''  and  aa.updatetime::date=(now() - interval ''1 day'')::date;

select * from topic.dwt_月报表统计信息 where  数量<>0;

--更新当日报表信息 --''AWP Rate''
insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期,''AWP Rate''  AS 统计类型,
   COALESCE(bb.数量,0)/ aa.数量 as 数量
   from topic.dws_每日统计信息 aa
   left outer join topic.dws_每日统计信息 bb
   on aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.日期 = bb.日期 and bb.统计类型 = ''AWP''
   where aa.统计类型 = ''WIP'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前日报表信息 --''WIP<=5WD Rate''
insert into topic.dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期,
 ''WIP<=5WD Rate'' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from topic.dws_每日统计信息 aa
 left outer join topic.dws_每日统计信息 bb on aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.日期 = bb.日期
 and bb.统计类型 = ''WIP<=5WD''
 where aa.统计类型 = ''WIP'' and aa.数量 > 0 and aa.updatetime::date=(now() - interval ''1 day'')::date;
';
execute update_daily;
mycode='success';
RETURN(mycode);
END
$$
language plpgsql;

drop function IF EXISTS Dell_KPI_Week();
create or replace function Dell_KPI_Week()
returns text as
$$
declare
    update_Week text;
    mycode text;
BEGIN
    update_Week:='insert into topic.dwt_周报表统计信息(地区, 种类, 财年, 周,统计类型, 数量)
select 地区, 种类, 财年, 周,  统计类型,  SUM(数量) as  数量
   from topic.dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval ''1 day'')::date
         and 统计类型 not in (''AverageTAT<=5wd(Goal:90%)'', ''Planning Yield (Goal:65%)'', ''NFF rate'',
    ''AWP Rate'', ''WIP<=5WD Rate'', ''AverageTAT Total<=5wd(Goal:90%)'')
   group by 地区, 种类, 财年, 周, 统计类型;

--更新周报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into topic.dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 周,''AverageTAT<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
     and updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 周
   ) aa
   left outer join topic.dwt_周报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
      and bb.统计类型 = ''Average TAT<=5wd'';

 --更新周报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into topic.dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 周,''AverageTAT Total<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where  统计类型 = ''Total'' and  updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 周
   ) aa
   left outer join topic.dwt_周报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
      and bb.统计类型 = ''Average TAT Total<=5wd'';

--更新周报表信息 -- Planning Yield (Goal:65%)
   insert into topic.dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周,   ''Planning Yield (Goal:65%)'' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_周报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 周, SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
   group by  地区, 种类, 财年, 周
   )  bb ON (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = ''RTV'' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = ''Total'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前周报表信息 -- NFF rate
   insert into topic.dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周, ''NFF rate'' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_周报表统计信息 aa
   LEFT OUTER JOIN topic.dwt_周报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
   and bb.统计类型 = ''NFF''
   where aa.统计类型 = ''Total''  and aa.updatetime::date=(now() - interval ''1 day'')::date;

select * from topic.dwt_周报表统计信息 where 统计类型=''WIP'';

-- 更新每周 ''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD''
update  topic.dwt_周报表统计信息 d周
 set 数量 =
case when d周.统计类型 = ''WIP'' then (COALESCE(bbb.Qty, 0))
     when d周.统计类型 = ''OnWay'' then  (COALESCE(bbb.OnWay, 0))
     when d周.统计类型 = ''AWP'' then  (COALESCE(bbb.AWP, 0))
     when d周.统计类型 = ''WIP<=5WD'' then  (COALESCE(bbb.Days_5, 0))
     when d周.统计类型 = ''WIP>5WD'' then  (COALESCE(bbb.Days_More5, 0))
     else ''-1''
    end
from topic.dwt_周报表统计信息 aaa
    left outer join
    (
    select   bb.fym, bb.fwk, aa.*
    from topic.dws_Report_DELL_Daily_WIP aa
    inner join
    (select fym,  fwk, MAX(fdate)::date as fDate
    from  public.dwd_dim_date_info bb
    group by fym,  fwk) bb on bb.fDate=updatetime::date
    ) bbb on (aaa.地区,aaa.种类,aaa.财年,aaa.周) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fwk)

where d周.统计类型 in  (''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD'' )
       and (d周.地区,d周.种类,d周.财年,d周.周) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fwk);
      /* and d周.updatetime= (select tmp.fDate from (select fym,  fwk, MAX(fdate)::date as fDate
                              from  public.dwd_dim_date_info
                              group by fym,  fwk) tmp);*/

--更新当周报表统计信息 --''AWP Rate''
insert into topic.dwt_周报表统计信息(地区, 种类, 财年,周, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,aa.周,''AWP Rate''  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from topic.dwt_周报表统计信息 aa
   left outer join topic.dwt_周报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = ''AWP''
       and aa.财年 = bb.财年 and aa.周 = bb.周
   where aa.统计类型 = ''WIP'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前周报表信息 --''WIP<=5WD Rate''
insert into topic.dwt_周报表统计信息(地区, 种类, 财年,周, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,aa.周,
 ''WIP<=5WD Rate'' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from topic.dwt_周报表统计信息 aa
 left outer join topic.dwt_周报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and aa.周 = bb.周
            and bb.统计类型 = ''WIP<=5WD''
 where aa.统计类型 = ''WIP''
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval ''1 day'')::date;';
execute update_Week;
mycode='success';
RETURN(mycode);
END
$$
language plpgsql;

drop function IF EXISTS Dell_KPI_Month();
create or replace function Dell_KPI_Month()
returns text as
$$
declare
    update_Month text;
    mycode text;
BEGIN
    update_Month:='insert into topic.dwt_月报表统计信息(地区, 种类, 财年,月度,统计类型, 数量)
select 地区, 种类, 财年,月度,  统计类型,  SUM(数量) as  数量
   from topic.dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval ''1 day'')::date
         and 统计类型 not in (''AverageTAT<=5wd(Goal:90%)'', ''Planning Yield (Goal:65%)'', ''NFF rate'',
    ''AWP Rate'', ''WIP<=5WD Rate'', ''AverageTAT Total<=5wd(Goal:90%)'')
   group by 地区, 种类, 财年, 月度, 统计类型;

--更新月报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into topic.dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,月度,''AverageTAT<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
     and updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 月度
   ) aa
   left outer join topic.dwt_月报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
      and bb.统计类型 = ''Average TAT<=5wd'';

 --更新月度报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into topic.dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 月度,''AverageTAT Total<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where  统计类型 = ''Total'' and  updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 月度
   ) aa
   left outer join topic.dwt_月报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
      and bb.统计类型 = ''Average TAT Total<=5wd'';

--更新月度报表信息 -- Planning Yield (Goal:65%)
   insert into topic.dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度,   ''Planning Yield (Goal:65%)'' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_月报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 月度, SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
   group by  地区, 种类, 财年, 月度
   )  bb ON (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = ''RTV'' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = ''Total'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前月度报表信息 -- NFF rate
   insert into topic.dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度, ''NFF rate'' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_月报表统计信息 aa
   LEFT OUTER JOIN topic.dwt_月报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
   and bb.统计类型 = ''NFF''
   where aa.统计类型 = ''Total''  and aa.updatetime::date=(now() - interval ''1 day'')::date;
select distinct 统计类型 from topic.dwt_月报表统计信息 ;

-- 更新每月 ''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD''
update  topic.dwt_月报表统计信息 d月
 set 数量 =
     case when d月.统计类型 = ''WIP'' then COALESCE(bbb.Qty, 0)
     when d月.统计类型 = ''OnWay'' then COALESCE(bbb.OnWay, 0)
     when d月.统计类型 = ''AWP'' then COALESCE(bbb.AWP, 0)
     when d月.统计类型 = ''WIP<=5WD'' then COALESCE(bbb.Days_5, 0)
     when d月.统计类型 = ''WIP>5WD'' then COALESCE(bbb.Days_More5, 0)
     else ''-1''
    end
from topic.dwt_月报表统计信息 aaa
left outer join
(
select   bb.fym, bb.fmm, aa.*
from topic.dws_Report_DELL_Daily_WIP aa
inner join
(select fym,  fmm, MAX(fdate)::date as fDate
from  public.dwd_dim_date_info bb
group by fym,  fmm) bb on bb.fDate=aa.updatetime::date
) bbb on (aaa.地区,aaa.种类,aaa.财年,aaa.月度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fmm)
where d月.统计类型 in  (''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD'' )
and (d月.地区,d月.种类,d月.财年,d月.月度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fmm);

select * from topic.dwt_月报表统计信息 where 统计类型=''WIP>5WD'';

--更新当月度报表统计信息 --''AWP Rate''
insert into topic.dwt_月报表统计信息(地区, 种类, 财年,月度, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,aa.月度,''AWP Rate''  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from topic.dwt_月报表统计信息 aa
   left outer join topic.dwt_月报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = ''AWP''
       and aa.财年 = bb.财年 and aa.月度 = bb.月度
   where aa.统计类型 = ''WIP'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前月报表信息 --''WIP<=5WD Rate''
insert into topic.dwt_月报表统计信息(地区, 种类, 财年,月度, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,aa.月度,
 ''WIP<=5WD Rate'' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from topic.dwt_月报表统计信息 aa
 left outer join topic.dwt_月报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and aa.月度 = bb.月度
            and bb.统计类型 = ''WIP<=5WD''
 where aa.统计类型 = ''WIP''
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval ''1 day'')::date;';
execute update_Month;
mycode='success';
RETURN(mycode);
END
$$
language plpgsql;

drop function IF EXISTS Dell_KPI_Quarter();
create or replace function Dell_KPI_Quarter()
returns text as
$$
declare
    update_Quarter text;
    mycode text;
BEGIN
    update_Quarter:='insert into topic.dwt_季度报表统计信息(地区, 种类, 财年,季度,统计类型, 数量)
select 地区,种类,财年,季度,统计类型,  SUM(数量) as  数量
   from topic.dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval ''1 day'')::date
         and 统计类型 not in (''AverageTAT<=5wd(Goal:90%)'', ''Planning Yield (Goal:65%)'', ''NFF rate'',
    ''AWP Rate'', ''WIP<=5WD Rate'', ''AverageTAT Total<=5wd(Goal:90%)'')
   group by 地区, 种类, 财年, 季度, 统计类型;
--更新季度报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into topic.dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,季度,''AverageTAT<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
     and updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 季度
   ) aa
   left outer join topic.dwt_季度报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
      and bb.统计类型 = ''Average TAT<=5wd'';

 --更新季度度报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into topic.dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 季度,''AverageTAT Total<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where  统计类型 = ''Total'' and  updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年, 季度
   ) aa
   left outer join topic.dwt_季度报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
      and bb.统计类型 = ''Average TAT Total<=5wd'';

--更新季度报表信息 -- Planning Yield (Goal:65%)
   insert into topic.dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度,   ''Planning Yield (Goal:65%)'' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_季度报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 季度, SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
   group by  地区, 种类, 财年, 季度
   )  bb ON (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = ''RTV'' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = ''Total'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前季度报表信息 -- NFF rate
   insert into topic.dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, ''NFF rate'' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_季度报表统计信息 aa
   LEFT OUTER JOIN topic.dwt_季度报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
   and bb.统计类型 = ''NFF''
   where aa.统计类型 = ''Total''  and aa.updatetime::date=(now() - interval ''1 day'')::date;

-- 更新每季度 ''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD''
update  topic.dwt_季度报表统计信息 d季
 set 数量 =
     case when d季.统计类型 = ''WIP'' then COALESCE(bbb.Qty, 0)
     when d季.统计类型 = ''OnWay'' then COALESCE(bbb.OnWay, 0)
     when d季.统计类型 = ''AWP'' then COALESCE(bbb.AWP, 0)
     when d季.统计类型 = ''WIP<=5WD'' then COALESCE(bbb.Days_5, 0)
     when d季.统计类型 = ''WIP>5WD'' then COALESCE(bbb.Days_More5, 0)
     else ''-1''
    end
from topic.dwt_季度报表统计信息 aaa
left outer join
(
select   bb.fym, bb.fqm, aa.*
from topic.dws_Report_DELL_Daily_WIP aa
inner join
(select fym,  fqm, MAX(fdate)::date as fDate
from  public.dwd_dim_date_info bb
group by fym,  fqm) bb on bb.fDate=aa.updatetime::date
) bbb on (aaa.地区,aaa.种类,aaa.财年,aaa.季度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fqm)
where d季.统计类型 in  (''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD'' )
and (d季.地区,d季.种类,d季.财年,d季.季度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fqm);

--更新当季度报表统计信息 --''AWP Rate''
insert into topic.dwt_季度报表统计信息(地区, 种类, 财年,季度, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,aa.季度,''AWP Rate''  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from topic.dwt_季度报表统计信息 aa
   left outer join topic.dwt_季度报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = ''AWP''
       and aa.财年 = bb.财年 and aa.季度 = bb.季度
   where aa.统计类型 = ''WIP'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前季度报表信息 --''WIP<=5WD Rate''
insert into topic.dwt_季度报表统计信息(地区, 种类, 财年,季度, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,aa.季度,
 ''WIP<=5WD Rate'' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from topic.dwt_季度报表统计信息 aa
 left outer join topic.dwt_季度报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and aa.季度 = bb.季度
            and bb.统计类型 = ''WIP<=5WD''
 where aa.统计类型 = ''WIP''
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval ''1 day'')::date;';
execute update_Quarter;
mycode='success';
RETURN(mycode);
END
$$
language plpgsql;

drop function IF EXISTS Dell_KPI_Year();
create or replace function Dell_KPI_Year()
returns text as
$$
declare
    update_Year text;
    mycode text;
BEGIN
    update_Year:='insert into topic.dwt_年度报表统计信息(地区, 种类, 财年,统计类型, 数量)
select 地区,种类,财年,统计类型,  SUM(数量) as  数量
   from topic.dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval ''1 day'')::date
         and 统计类型 not in (''AverageTAT<=5wd(Goal:90%)'', ''Planning Yield (Goal:65%)'', ''NFF rate'',
    ''AWP Rate'', ''WIP<=5WD Rate'', ''AverageTAT Total<=5wd(Goal:90%)'')
   group by 地区, 种类, 财年, 统计类型;

--更新年度报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into topic.dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,''AverageTAT<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
     and updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年
   ) aa
   left outer join topic.dwt_年度报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
      and bb.统计类型 = ''Average TAT<=5wd'';

 --更新年度度报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into topic.dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,''AverageTAT Total<=5wd(Goal:90%)'' as 统计类型,  SUM(数量) as 数量
   from topic.dws_每日统计信息
   where  统计类型 = ''Total'' and  updatetime::date=(now() - interval ''1 day'')::date
   group by  地区, 种类, 财年
   ) aa
   left outer join topic.dwt_年度报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
      and bb.统计类型 = ''Average TAT Total<=5wd'';

--更新年度报表信息 -- Planning Yield (Goal:65%)
   insert into topic.dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,  ''Planning Yield (Goal:65%)'' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_年度报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年,SUM(数量) as 数量
   from topic.dws_每日统计信息
   where 统计类型 in (''NFF'', ''FG'' )
   group by  地区, 种类, 财年
   )  bb ON (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = ''RTV'' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = ''Total'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前年度报表信息 -- NFF rate
   insert into topic.dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, ''NFF rate'' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from topic.dwt_年度报表统计信息 aa
   LEFT OUTER JOIN topic.dwt_年度报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
   and bb.统计类型 = ''NFF''
   where aa.统计类型 = ''Total''  and aa.updatetime::date=(now() - interval ''1 day'')::date;

-- 更新每年 ''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD'' 一周结束统计
-- 更新每季度 ''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD''
update  topic.dwt_年度报表统计信息 d年
 set 数量 =
     case when d年.统计类型 = ''WIP'' then COALESCE(bbb.Qty, 0)
     when d年.统计类型 = ''OnWay'' then COALESCE(bbb.OnWay, 0)
     when d年.统计类型 = ''AWP'' then COALESCE(bbb.AWP, 0)
     when d年.统计类型 = ''WIP<=5WD'' then COALESCE(bbb.Days_5, 0)
     when d年.统计类型 = ''WIP>5WD'' then COALESCE(bbb.Days_More5, 0)
     else ''-1''
    end
from topic.dwt_年度报表统计信息 aaa
left outer join
(
select   bb.fym, aa.*
from topic.dws_Report_DELL_Daily_WIP aa
inner join
(select fym,MAX(fdate)::date as fDate
from  public.dwd_dim_date_info bb
group by fym) bb on bb.fDate=updatetime::date
) bbb on (aaa.地区,aaa.种类,aaa.财年) = (bbb.AREA,bbb.Commodity,bbb.fym)
where d年.统计类型 in  (''WIP'', ''OnWay'', ''AWP'', ''WIP<=5WD'', ''WIP>5WD'' )
and (d年.地区,d年.种类,d年.财年) = (bbb.AREA,bbb.Commodity,bbb.fym);

--更新当年度报表统计信息 --''AWP Rate''
insert into topic.dwt_年度报表统计信息(地区, 种类, 财年, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,''AWP Rate''  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from topic.dwt_年度报表统计信息 aa
   left outer join topic.dwt_年度报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = ''AWP''
       and aa.财年 = bb.财年
   where aa.统计类型 = ''WIP'' and  aa.updatetime::date=(now() - interval ''1 day'')::date;

--更新当前年度报表信息 --''WIP<=5WD Rate''
insert into topic.dwt_年度报表统计信息(地区, 种类, 财年, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,
 ''WIP<=5WD Rate'' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from topic.dwt_年度报表统计信息 aa
 left outer join topic.dwt_年度报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and bb.统计类型 = ''WIP<=5WD''
 where aa.统计类型 = ''WIP''
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval ''1 day'')::date;';
execute update_Year;
mycode='success';
RETURN(mycode);
END
$$
language plpgsql;

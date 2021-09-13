/***************************************************每日统计信息主题表（天）***************************************************/
/*                                                                                                                    */
/*                                                                                                                    */
/**********************************************************************************************************************/
/*建表*/
DROP TABLE IF EXISTS dws_每日统计信息;
CREATE TABLE dws_每日统计信息(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    地区 varchar(30) ,
	种类 varchar(50) ,
	财年 varchar(10) NULL,
	季度 int NULL,
	月度 int NULL,
	周 int NULL,
	日期 timestamp ,
	统计类型 varchar(50) ,
	数量 decimal(18, 4) ,
	updatetime timestamp default (now() - interval '1 day')
    )Distributed by (ID);
/*Received*/
insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate,'Received' as fType ,COALESCE(bb.qty,0) as QTY
  from public.dwd_dim_base_commodity_daily aa
  inner join
  (
    select Area, Commodity, Receive_Date, COUNT(1) as Qty
    from public.dwd_fact_orderdetail
    where updatetime::date=(now() - interval '1 day')::date
    group by Area, Commodity, Receive_Date
  ) bb on aa.area = bb.area and aa.Commodity = bb.Commodity and aa.fdate=bb.Receive_Date
where updatetime::date=(now() - interval '1 day')::date;
/*Close*/
insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, bb.fType, COALESCE(bb.Qty,0) as Qty
from public.dwd_dim_base_commodity_daily aa
inner join
  (
    select Area, Commodity, Close_Date, Final_Result as fType,  COUNT(1) as Qty
    from public.dwd_fact_close_base_orderdetail
    where updatetime::date=(now() - interval '1 day')::date
    group by Area, Commodity, Close_Date, Final_Result

    union all

    select Area, Commodity, Close_Date, Final_Result as fType,  COUNT(1) as Qty
    from public.dwd_fact_close_monitor_orderdetail
    where updatetime::date=(now() - interval '1 day')::date
    group by Area, Commodity, Close_Date, Final_Result

    union all

    select Area, Commodity, Close_Date, Final_Result as fType,  COUNT(1) as Qty
    from public.dwd_fact_close_panel_orderdetail
    where updatetime::date=(now() - interval '1 day')::date
    group by Area, Commodity, Close_Date, Final_Result
   ) bb on aa.area= bb.Area and aa.commodity=bb.Commodity and aa.fdate=bb.Close_Date
   where updatetime::date=(now() - interval '1 day')::date;
/*Closed  Average TAT<=5wd*/
    insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
    select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, bb.fType,COALESCE(bb.Qty,0) as Qty
    from public.dwd_dim_base_commodity_daily aa
    inner join
    (
      select Area, Commodity, Close_Date, 'Average TAT<=5wd' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_base_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity)
        and Final_Result in ('NFF', 'FG')
        and updatetime::date=(now() - interval '1 day')::date
      group by Area, Commodity, Close_Date
      union all
    select Area, Commodity, Close_Date, 'Average TAT<=5wd' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_monitor_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity)
        and Final_Result in ('NFF', 'FG')
        and updatetime::date=(now() - interval '1 day')::date
      group by Area, Commodity, Close_Date
      union all
     select Area, Commodity, Close_Date, 'Average TAT<=5wd' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_panel_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity)
        and Final_Result in ('NFF', 'FG')
        and updatetime::date=(now() - interval '1 day')::date
      group by Area, Commodity, Close_Date
     )  bb on aa.area= bb.Area and aa.commodity=bb.Commodity and aa.fdate=bb.Close_Date
      where updatetime::date=(now() - interval '1 day')::date;

/*Closed  Average TAT Total<=5wd*/
    insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
    select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, bb.fType,COALESCE(bb.Qty,0) as Qty
    from public.dwd_dim_base_commodity_daily aa
    inner join
    (
      select Area, Commodity, Close_Date, 'Average TAT Total<=5wd' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_base_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity) -- and Final_Result in ('NFF', 'FG')
      and updatetime::date=(now() - interval '1 day')::date
      group by Area, Commodity, Close_Date
      union all
      select Area, Commodity, Close_Date, 'Average TAT Total<=5wd' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_monitor_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity) -- and Final_Result in ('NFF', 'FG')
      and updatetime::date=(now() - interval '1 day')::date
      group by Area, Commodity, Close_Date
      union all
      select Area, Commodity, Close_Date, 'Average TAT Total<=5wd' as fType,  COUNT(1) as Qty
      from public.dwd_fact_close_panel_orderdetail
      where COALESCE(TAT_sys, TAT_Closed) <= public.Get_WIP_Goal(Commodity) -- and Final_Result in ('NFF', 'FG')
      and updatetime::date=(now() - interval '1 day')::date
      group by Area, Commodity, Close_Date
     )  bb on aa.area= bb.Area and aa.commodity=bb.Commodity and aa.fdate=bb.Close_Date
      where updatetime::date=(now() - interval '1 day')::date;

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, 'WIP' as fType ,COALESCE(bb.qty,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
       and aa.commodity = bb.Commodity
       and aa.fdate::date= bb.updatetime::date
       and bb.Qty > 0
   where aa.updatetime::date=(now() - interval '1 day')::date;

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, 'OnWay' as fType ,COALESCE(bb.OnWay,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
       and aa.commodity = bb.Commodity
       and aa.fdate::date= bb.updatetime::date
       and bb.onWay > 0
   where aa.updatetime::date=(now() - interval '1 day')::date;

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, 'WIP<=5WD' as fType ,COALESCE(bb.Days_5,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
       and  aa.commodity = bb.Commodity
       and aa.fdate::date= bb.updatetime::date
       and COALESCE(bb.Days_5, 0) > 0
   where aa.updatetime::date=(now() - interval '1 day')::date;

insert into  dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, 'WIP>5WD' as fType ,COALESCE(bb.Days_More5,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
              and  aa.commodity = bb.Commodity
              --and aa.fdate= bb.updatetime
   and COALESCE(bb.Days_More5, 0) > 0
   where aa.updatetime::date=(now() - interval '1 day')::date;


insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.area,aa.commodity,aa.fym,aa.fqm,aa.fmm,aa.fwm,aa.fdate, 'AWP' as fType ,COALESCE(bb.AWP,0) as QTY
   from public.dwd_dim_base_commodity_daily aa
   inner join dws_Report_DELL_Daily_WIP bb
       on aa.area = bb.AREA
              and  aa.commodity = bb.Commodity
              and aa.fdate::date=bb.updatetime::date
   and COALESCE(bb.AWP, 0) > 0
   where aa.updatetime::date=(now() - interval '1 day')::date;

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区,aa.种类,aa.财年,aa.季度,aa.月度,aa.周,aa.日期,'Total' as 统计类型,  SUM(aa.数量) as 数量
   from dws_每日统计信息 aa
   where 种类 in  ('MEM','CPU', 'HDD', 'CARD')
   and 统计类型 in ('NFF', 'FG', 'Scrap', 'RTV')
   and  aa.updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期;

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区,aa.种类,aa.财年,aa.季度,aa.月度,aa.周,aa.日期, 'Total' as 统计类型,  SUM(aa.数量) as 数量
   from dws_每日统计信息 aa
   where  种类 not in  ('MEM','CPU', 'HDD', 'CARD')
   and 统计类型 in ('NFF', 'FG', 'Scrap', 'Internal use' )
   and  aa.updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期;

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区,aa.种类,aa.财年,aa.季度,aa.月度,aa.周,aa.日期, aa.统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 季度, 月度, 周, 日期, 'AverageTAT<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' ) and  updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期
   ) aa
   left outer join dws_每日统计信息 bb on aa.地区 = bb.地区 and aa.种类 = bb.种类
   and aa.财年= bb.财年 and aa.日期 = bb.日期 and bb.统计类型 = 'Average TAT<=5wd';

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期, aa.统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/ aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 季度, 月度, 周, 日期, 'AverageTAT Total<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where  统计类型 = 'Total' and updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期
   ) aa
   left outer join dws_每日统计信息 bb on aa.地区 = bb.地区 and aa.种类 = bb.种类
   and aa.财年 = bb.财年 and aa.日期 = bb.日期 and bb.统计类型 = 'Average TAT Total<=5wd';

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期, 'Planning Yield (Goal:65%)' as 统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/ aa.数量) else 0 end as 数量
   from dws_每日统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 季度, 月度, 周, 日期, SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
   group by  地区, 种类, 财年, 季度, 月度, 周, 日期
   )  bb ON aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.财年 = bb.财年 and aa.日期 = bb.日期
 --  left outer join DELL_KPI_Daily_item cc
  -- ON cc.fType = 'RTV' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym AND AA.fDate = CC.fDate
   where aa.统计类型 = 'Total'   and  updatetime::date=(now() - interval '1 day')::date;

insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期, 'NFF rate' as 统计类型,
   case when aa.数量 > 0 then (COALESCE(bb.数量,0)/ aa.数量) else 0 end as 数量
   from dws_每日统计信息 aa
   LEFT OUTER JOIN dws_每日统计信息 bb
   ON aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.财年 = bb.财年 and aa.日期 = bb.日期
   and bb.统计类型 = 'NFF'
   where aa.统计类型 = 'Total'  and  aa.updatetime::date=(now() - interval '1 day')::date;

select * from dwt_月报表统计信息 where  数量<>0;

--更新当日报表信息 --'AWP Rate'
insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期,'AWP Rate'  AS 统计类型,
   COALESCE(bb.数量,0)/ aa.数量 as 数量
   from dws_每日统计信息 aa
   left outer join dws_每日统计信息 bb
   on aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.日期 = bb.日期 and bb.统计类型 = 'AWP'
   where aa.统计类型 = 'WIP' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前日报表信息 --'WIP<=5WD Rate'
insert into dws_每日统计信息 (地区, 种类, 财年, 季度, 月度, 周, 日期, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年, aa.季度, aa.月度, aa.周, aa.日期,
 'WIP<=5WD Rate' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from dws_每日统计信息 aa
 left outer join dws_每日统计信息 bb on aa.地区 = bb.地区 and aa.种类 = bb.种类 and aa.日期 = bb.日期
 and bb.统计类型 = 'WIP<=5WD'
 where aa.统计类型 = 'WIP' and aa.数量 > 0 and aa.updatetime::date=(now() - interval '1 day')::date;


SELECT * FROM dws_每日统计信息 WHERE 统计类型='Total';


/***************************************************Report_DELL_Daily_WIP****************************************************/
DROP TABLE IF EXISTS dws_Report_DELL_Daily_WIP;
CREATE TABLE dws_Report_DELL_Daily_WIP(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
	AREA varchar(20) ,
	Commodity varchar(50) ,
	Date_Wip timestamp,
	Qty int,
	Days_5 int ,
	Days_More5 int ,
	OnWay int ,
	AWP int ,
	cDate timestamp,
	Date_Update timestamp,
	updatetime timestamp default (now() - interval '1 day')
)Distributed by (ID);

INSERT INTO dws_Report_DELL_Daily_WIP(area, commodity, qty, days_5, days_more5)
select AREA, Commodity,COUNT(1) as Qty, 0 as Days_5, 0 as Days_More5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
    and RmaSta > 0
    and updatetime::date=(now() - interval '1 day')::date
  --  where Receive_Date is not null
    group by AREA, Commodity;

update dws_Report_DELL_Daily_WIP set Days_5 = bb.Days5
    from dws_Report_DELL_Daily_WIP aa
    inner join (
    select AREA, Commodity,updatetime, COUNT(1) as Days5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta > 0
      and AgingDays <= public.Get_WIP_Goal(Commodity)
      and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
      and updatetime::date=(now() - interval '1 day')::date
    group by AREA, Commodity,updatetime
    )     bb on aa.area = bb.AREA and bb.Commodity = aa.Commodity and aa.updatetime::date = bb.updatetime::date;

update dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Days_5) =(
	select bb.AREA,bb.Commodity,bb.Days5 from
    (select AREA, Commodity,updatetime, COUNT(1) as Days5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta > 0
      and AgingDays <= public.Get_WIP_Goal(Commodity)
      and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
      and updatetime::date=(now() - interval '1 day')::date
    group by AREA, Commodity,updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta > 0
                             and AgingDays <= public.Get_WIP_Goal(Commodity)
                             and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
                             and updatetime::date=(now() - interval '1 day')::date
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval '1 day')::date;

update dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Days_5) =(
	select bb.AREA,bb.Commodity,aa.days_5 + bb.Days5 from
    (select AREA, Commodity,updatetime, COUNT(1) as Days5
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta > 0
      and Receive_Date is null
      and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
    group by AREA, Commodity,updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta > 0
                             and Receive_Date is null
                             and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval '1 day')::date;

update dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Days_More5) =(
	select bb.AREA,bb.Commodity,bb.Days_More5 from
    (select AREA, Commodity, updatetime, COUNT(1) as Days_More5
        from public.dwd_fact_report_dell_kpi_item_wip
        where RmaSta >0
          and AgingDays > public.Get_WIP_Goal(Commodity)
          and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
        group by AREA, Commodity,updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta >0
                             and AgingDays > public.Get_WIP_Goal(Commodity)
                             and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval '1 day')::date;

update dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,Onway) =(
	select bb.AREA,bb.Commodity, bb.OnWay from
    (select AREA, Commodity, updatetime, COUNT(1) as OnWay
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta = 0 and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
    group by AREA, Commodity, updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta = 0 and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
    group by AREA, Commodity, updatetime)
      and updatetime::date=(now() - interval '1 day')::date;

update dws_Report_DELL_Daily_WIP aa set (AREA,Commodity,AWP) =(
	select bb.AREA,bb.Commodity,bb.AWP from
    (select AREA, Commodity, updatetime, COUNT(1) as AWP
    from public.dwd_fact_report_dell_kpi_item_wip
    where RmaSta >0
      and "Is_On hold" = 'Y'
      and DelayType = 'Material shortage'
      and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
    group by AREA, Commodity, updatetime) bb
    where aa.area = bb.AREA
                and bb.Commodity = aa.Commodity
                and aa.updatetime::date = bb.updatetime::date)
where (AREA,Commodity) in (select AREA, Commodity
                           from public.dwd_fact_report_dell_kpi_item_wip
                           where RmaSta >0
                             and "Is_On hold" = 'Y'
                             and DelayType = 'Material shortage'
                             and RETURNTO_RMA not in  ('CJSPHL', 'CJINUSE')
                           group by AREA, Commodity)
                and updatetime::date=(now() - interval '1 day')::date;

/***************************************************周报表信息****************************************************/
DROP TABLE IF EXISTS dwt_周报表统计信息;
CREATE TABLE dwt_周报表统计信息(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    地区 varchar(30) ,
	种类 varchar(50) ,
	财年 varchar(10),
	周 int ,
	统计类型 varchar(50) ,
	数量 decimal(18, 4) ,
	updatetime timestamp default (now() - interval '1 day')
    )Distributed by (ID);

insert into dwt_周报表统计信息(地区, 种类, 财年, 周,统计类型, 数量)
select 地区, 种类, 财年, 周,  统计类型,  SUM(数量) as  数量
   from dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval '1 day')::date
         and 统计类型 not in ('AverageTAT<=5wd(Goal:90%)', 'Planning Yield (Goal:65%)', 'NFF rate',
    'AWP Rate', 'WIP<=5WD Rate', 'AverageTAT Total<=5wd(Goal:90%)')
   group by 地区, 种类, 财年, 周, 统计类型;

--更新周报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 周,'AverageTAT<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
     and updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 周
   ) aa
   left outer join dwt_周报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
      and bb.统计类型 = 'Average TAT<=5wd';

 --更新周报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 周,'AverageTAT Total<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where  统计类型 = 'Total' and  updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 周
   ) aa
   left outer join dwt_周报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
      and bb.统计类型 = 'Average TAT Total<=5wd';

--更新周报表信息 -- Planning Yield (Goal:65%)
   insert into dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周,   'Planning Yield (Goal:65%)' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_周报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 周, SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
   group by  地区, 种类, 财年, 周
   )  bb ON (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = 'RTV' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = 'Total' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前周报表信息 -- NFF rate
   insert into dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.周, 'NFF rate' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_周报表统计信息 aa
   LEFT OUTER JOIN dwt_周报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年,aa.周) = (bb.地区,bb.种类,bb.财年,bb.周)
   and bb.统计类型 = 'NFF'
   where aa.统计类型 = 'Total'  and aa.updatetime::date=(now() - interval '1 day')::date;

select * from dwt_周报表统计信息 where 统计类型='WIP';

-- 更新每周 'WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD'
update  dwt_周报表统计信息 d周
 set 数量 =
case when d周.统计类型 = 'WIP' then (COALESCE(bbb.Qty, 0))
     when d周.统计类型 = 'OnWay' then  (COALESCE(bbb.OnWay, 0))
     when d周.统计类型 = 'AWP' then  (COALESCE(bbb.AWP, 0))
     when d周.统计类型 = 'WIP<=5WD' then  (COALESCE(bbb.Days_5, 0))
     when d周.统计类型 = 'WIP>5WD' then  (COALESCE(bbb.Days_More5, 0))
     else '-1'
    end
from dwt_周报表统计信息 aaa
    left outer join
    (
    select   bb.fym, bb.fwk, aa.*
    from dws_Report_DELL_Daily_WIP aa
    inner join
    (select fym,  fwk, MAX(fdate)::date as fDate
    from  public.dwd_dim_date_info bb
    group by fym,  fwk) bb on bb.fDate=updatetime::date
    ) bbb on (aaa.地区,aaa.种类,aaa.财年,aaa.周) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fwk)

where d周.统计类型 in  ('WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD' )
       and (d周.地区,d周.种类,d周.财年,d周.周) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fwk);
      /* and d周.updatetime= (select tmp.fDate from (select fym,  fwk, MAX(fdate)::date as fDate
                              from  public.dwd_dim_date_info
                              group by fym,  fwk) tmp);*/

--更新当周报表统计信息 --'AWP Rate'
insert into dwt_周报表统计信息(地区, 种类, 财年,周, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,aa.周,'AWP Rate'  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from dwt_周报表统计信息 aa
   left outer join dwt_周报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = 'AWP'
       and aa.财年 = bb.财年 and aa.周 = bb.周
   where aa.统计类型 = 'WIP' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前周报表信息 --'WIP<=5WD Rate'
insert into dwt_周报表统计信息(地区, 种类, 财年,周, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,aa.周,
 'WIP<=5WD Rate' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from dwt_周报表统计信息 aa
 left outer join dwt_周报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and aa.周 = bb.周
            and bb.统计类型 = 'WIP<=5WD'
 where aa.统计类型 = 'WIP'
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval '1 day')::date;





/*
/*更新新增*/
DROP TABLE IF EXISTS temp_forwk;
CREATE TEMPORARY TABLE temp_forwk(
    地区 varchar(30) ,
	种类 varchar(50) ,
	财年 varchar(10),
	周 int,
	统计类型 varchar(50),
	数量 decimal(18, 4));

insert into temp_forwk(地区, 种类, 财年, 周,统计类型, 数量)
select 地区, 种类, 财年, 周,  统计类型, SUM(数量) as  数量
   from dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval '1 day')::date
   group by 地区, 种类, 财年, 周, 统计类型;

UPDATE dwt_周报表统计信息  aa
SET 数量=(
    select (aa.数量+ bb.数量) AS 数量
    from temp_forwk bb
    where (aa.地区,aa.种类,aa.财年,aa.周,aa.统计类型) = (bb.地区,bb.种类,bb.财年,bb.周,bb.统计类型))
WHERE (aa.地区,aa.种类,aa.财年,aa.周,aa.统计类型) IN (SELECT bb.地区,bb.种类,bb.财年,bb.周,bb.统计类型 FROM temp_forwk bb);
set enable_nestloop =off;
INSERT INTO dwt_周报表统计信息  (地区, 种类, 财年, 周,统计类型, 数量)
select bb.地区, bb.种类, bb.财年, bb.周, bb.统计类型, bb.数量
from temp_forwk bb, dwt_周报表统计信息 aa
where (bb.财年,bb.周)<>(aa.财年,aa.周)
group by bb.地区, bb.种类, bb.财年, bb.周, bb.统计类型, bb.数量;
*/


/***************************************************月报表信息****************************************************/
DROP TABLE IF EXISTS dwt_月报表统计信息;
CREATE TABLE dwt_月报表统计信息(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    地区 varchar(30) ,
	种类 varchar(50) ,
	财年 varchar(10),
	月度 int ,
	统计类型 varchar(50) ,
	数量 decimal(18, 4) ,
	updatetime timestamp default (now() - interval '1 day')
    )Distributed by (ID);

insert into dwt_月报表统计信息(地区, 种类, 财年,月度,统计类型, 数量)
select 地区, 种类, 财年,月度,  统计类型,  SUM(数量) as  数量
   from dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval '1 day')::date
         and 统计类型 not in ('AverageTAT<=5wd(Goal:90%)', 'Planning Yield (Goal:65%)', 'NFF rate',
    'AWP Rate', 'WIP<=5WD Rate', 'AverageTAT Total<=5wd(Goal:90%)')
   group by 地区, 种类, 财年, 月度, 统计类型;

--更新月报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,月度,'AverageTAT<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
     and updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 月度
   ) aa
   left outer join dwt_月报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
      and bb.统计类型 = 'Average TAT<=5wd';

 --更新月度报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 月度,'AverageTAT Total<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where  统计类型 = 'Total' and  updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 月度
   ) aa
   left outer join dwt_月报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
      and bb.统计类型 = 'Average TAT Total<=5wd';

--更新月度报表信息 -- Planning Yield (Goal:65%)
   insert into dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度,   'Planning Yield (Goal:65%)' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_月报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 月度, SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
   group by  地区, 种类, 财年, 月度
   )  bb ON (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = 'RTV' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = 'Total' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前月度报表信息 -- NFF rate
   insert into dwt_月报表统计信息  (地区, 种类, 财年, 月度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.月度, 'NFF rate' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_月报表统计信息 aa
   LEFT OUTER JOIN dwt_月报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年,aa.月度) = (bb.地区,bb.种类,bb.财年,bb.月度)
   and bb.统计类型 = 'NFF'
   where aa.统计类型 = 'Total'  and aa.updatetime::date=(now() - interval '1 day')::date;
select distinct 统计类型 from dwt_月报表统计信息 ;

-- 更新每月 'WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD'
update  dwt_月报表统计信息 d月
 set 数量 =
     case when d月.统计类型 = 'WIP' then COALESCE(bbb.Qty, 0)
     when d月.统计类型 = 'OnWay' then COALESCE(bbb.OnWay, 0)
     when d月.统计类型 = 'AWP' then COALESCE(bbb.AWP, 0)
     when d月.统计类型 = 'WIP<=5WD' then COALESCE(bbb.Days_5, 0)
     when d月.统计类型 = 'WIP>5WD' then COALESCE(bbb.Days_More5, 0)
     else '-1'
    end
from dwt_月报表统计信息 aaa
left outer join
(
select   bb.fym, bb.fmm, aa.*
from dws_Report_DELL_Daily_WIP aa
inner join
(select fym,  fmm, MAX(fdate)::date as fDate
from  public.dwd_dim_date_info bb
group by fym,  fmm) bb on bb.fDate=aa.updatetime::date
) bbb on (aaa.地区,aaa.种类,aaa.财年,aaa.月度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fmm)
where d月.统计类型 in  ('WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD' )
and (d月.地区,d月.种类,d月.财年,d月.月度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fmm);

select * from dwt_月报表统计信息 where 统计类型='WIP>5WD';

--更新当月度报表统计信息 --'AWP Rate'
insert into dwt_月报表统计信息(地区, 种类, 财年,月度, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,aa.月度,'AWP Rate'  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from dwt_月报表统计信息 aa
   left outer join dwt_月报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = 'AWP'
       and aa.财年 = bb.财年 and aa.月度 = bb.月度
   where aa.统计类型 = 'WIP' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前月报表信息 --'WIP<=5WD Rate'
insert into dwt_月报表统计信息(地区, 种类, 财年,月度, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,aa.月度,
 'WIP<=5WD Rate' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from dwt_月报表统计信息 aa
 left outer join dwt_月报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and aa.月度 = bb.月度
            and bb.统计类型 = 'WIP<=5WD'
 where aa.统计类型 = 'WIP'
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval '1 day')::date;
/***************************************************季度报表信息****************************************************/
DROP TABLE IF EXISTS dwt_季度报表统计信息;
CREATE TABLE dwt_季度报表统计信息(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    地区 varchar(30) ,
	种类 varchar(50) ,
	财年 varchar(10),
	季度 int ,
	统计类型 varchar(50) ,
	数量 decimal(18, 4) ,
	updatetime timestamp default (now() - interval '1 day')
    )Distributed by (ID);

insert into dwt_季度报表统计信息(地区, 种类, 财年,季度,统计类型, 数量)
select 地区,种类,财年,季度,统计类型,  SUM(数量) as  数量
   from dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval '1 day')::date
         and 统计类型 not in ('AverageTAT<=5wd(Goal:90%)', 'Planning Yield (Goal:65%)', 'NFF rate',
    'AWP Rate', 'WIP<=5WD Rate', 'AverageTAT Total<=5wd(Goal:90%)')
   group by 地区, 种类, 财年, 季度, 统计类型;
--更新季度报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,季度,'AverageTAT<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
     and updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 季度
   ) aa
   left outer join dwt_季度报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
      and bb.统计类型 = 'Average TAT<=5wd';

 --更新季度度报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年, 季度,'AverageTAT Total<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where  统计类型 = 'Total' and  updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年, 季度
   ) aa
   left outer join dwt_季度报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
      and bb.统计类型 = 'Average TAT Total<=5wd';

--更新季度报表信息 -- Planning Yield (Goal:65%)
   insert into dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度,   'Planning Yield (Goal:65%)' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_季度报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年, 季度, SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
   group by  地区, 种类, 财年, 季度
   )  bb ON (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = 'RTV' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = 'Total' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前季度报表信息 -- NFF rate
   insert into dwt_季度报表统计信息  (地区, 种类, 财年, 季度,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.季度, 'NFF rate' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_季度报表统计信息 aa
   LEFT OUTER JOIN dwt_季度报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年,aa.季度) = (bb.地区,bb.种类,bb.财年,bb.季度)
   and bb.统计类型 = 'NFF'
   where aa.统计类型 = 'Total'  and aa.updatetime::date=(now() - interval '1 day')::date;

-- 更新每季度 'WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD'
update  dwt_季度报表统计信息 d季
 set 数量 =
     case when d季.统计类型 = 'WIP' then COALESCE(bbb.Qty, 0)
     when d季.统计类型 = 'OnWay' then COALESCE(bbb.OnWay, 0)
     when d季.统计类型 = 'AWP' then COALESCE(bbb.AWP, 0)
     when d季.统计类型 = 'WIP<=5WD' then COALESCE(bbb.Days_5, 0)
     when d季.统计类型 = 'WIP>5WD' then COALESCE(bbb.Days_More5, 0)
     else '-1'
    end
from dwt_季度报表统计信息 aaa
left outer join
(
select   bb.fym, bb.fqm, aa.*
from dws_Report_DELL_Daily_WIP aa
inner join
(select fym,  fqm, MAX(fdate)::date as fDate
from  public.dwd_dim_date_info bb
group by fym,  fqm) bb on bb.fDate=aa.updatetime::date
) bbb on (aaa.地区,aaa.种类,aaa.财年,aaa.季度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fqm)
where d季.统计类型 in  ('WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD' )
and (d季.地区,d季.种类,d季.财年,d季.季度) = (bbb.AREA,bbb.Commodity,bbb.fym,bbb.fqm);

--更新当季度报表统计信息 --'AWP Rate'
insert into dwt_季度报表统计信息(地区, 种类, 财年,季度, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,aa.季度,'AWP Rate'  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from dwt_季度报表统计信息 aa
   left outer join dwt_季度报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = 'AWP'
       and aa.财年 = bb.财年 and aa.季度 = bb.季度
   where aa.统计类型 = 'WIP' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前季度报表信息 --'WIP<=5WD Rate'
insert into dwt_季度报表统计信息(地区, 种类, 财年,季度, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,aa.季度,
 'WIP<=5WD Rate' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from dwt_季度报表统计信息 aa
 left outer join dwt_季度报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and aa.季度 = bb.季度
            and bb.统计类型 = 'WIP<=5WD'
 where aa.统计类型 = 'WIP'
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval '1 day')::date;

select distinct 统计类型 from dwt_季度报表统计信息;

/***************************************************年度报表信息****************************************************/
DROP TABLE IF EXISTS dwt_年度报表统计信息;
CREATE TABLE dwt_年度报表统计信息(
    ID INT  NOT NULL generated always as identity (START WITH 1 INCREMENT BY 1),
    地区 varchar(30) ,
	种类 varchar(50) ,
	财年 varchar(10),
	统计类型 varchar(50) ,
	数量 decimal(18, 4) ,
	updatetime timestamp default (now() - interval '1 day')
    )Distributed by (ID);

insert into dwt_年度报表统计信息(地区, 种类, 财年,统计类型, 数量)
select 地区,种类,财年,统计类型,  SUM(数量) as  数量
   from dws_每日统计信息 aa
   where aa.updatetime::date=(now() - interval '1 day')::date
         and 统计类型 not in ('AverageTAT<=5wd(Goal:90%)', 'Planning Yield (Goal:65%)', 'NFF rate',
    'AWP Rate', 'WIP<=5WD Rate', 'AverageTAT Total<=5wd(Goal:90%)')
   group by 地区, 种类, 财年, 统计类型;

--更新年度报表信息 -- AverageTAT<=5wd(Goal:90%)
insert into dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,'AverageTAT<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
     and updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年
   ) aa
   left outer join dwt_年度报表统计信息 bb
       on (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
      and bb.统计类型 = 'Average TAT<=5wd';

 --更新年度度报表信息 -- AverageTAT Total<=5wd(Goal:90%)
 insert into dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, aa.统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from
   (
   select 地区, 种类, 财年,'AverageTAT Total<=5wd(Goal:90%)' as 统计类型,  SUM(数量) as 数量
   from dws_每日统计信息
   where  统计类型 = 'Total' and  updatetime::date=(now() - interval '1 day')::date
   group by  地区, 种类, 财年
   ) aa
   left outer join dwt_年度报表统计信息 bb
      on (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
      and bb.统计类型 = 'Average TAT Total<=5wd';

--更新年度报表信息 -- Planning Yield (Goal:65%)
   insert into dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,  'Planning Yield (Goal:65%)' as 统计类型,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_年度报表统计信息 aa
   LEFT OUTER JOIN
   (
   select 地区, 种类, 财年,SUM(数量) as 数量
   from dws_每日统计信息
   where 统计类型 in ('NFF', 'FG' )
   group by  地区, 种类, 财年
   )  bb ON (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
--   left outer join DELL_KPI_WEEK_item cc
 --  ON cc.fType = 'RTV' and AA.Area = CC.Area AND AA.commodity = CC.commodity AND AA.fym=CC.fym  AND AA.fwk = cc.fwk
   where aa.统计类型 = 'Total' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前年度报表信息 -- NFF rate
   insert into dwt_年度报表统计信息  (地区, 种类, 财年,统计类型, 数量)
   select aa.地区, aa.种类, aa.财年, 'NFF rate' as fType,
   case when aa.数量 > 0 then  (COALESCE(bb.数量,0) / aa.数量) else 0 end as 数量
   from dwt_年度报表统计信息 aa
   LEFT OUTER JOIN dwt_年度报表统计信息 bb
   ON (aa.地区,aa.种类,aa.财年) = (bb.地区,bb.种类,bb.财年)
   and bb.统计类型 = 'NFF'
   where aa.统计类型 = 'Total'  and aa.updatetime::date=(now() - interval '1 day')::date;

-- 更新每年 'WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD' 一周结束统计
-- 更新每季度 'WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD'
update  dwt_年度报表统计信息 d年
 set 数量 =
     case when d年.统计类型 = 'WIP' then COALESCE(bbb.Qty, 0)
     when d年.统计类型 = 'OnWay' then COALESCE(bbb.OnWay, 0)
     when d年.统计类型 = 'AWP' then COALESCE(bbb.AWP, 0)
     when d年.统计类型 = 'WIP<=5WD' then COALESCE(bbb.Days_5, 0)
     when d年.统计类型 = 'WIP>5WD' then COALESCE(bbb.Days_More5, 0)
     else '-1'
    end
from dwt_年度报表统计信息 aaa
left outer join
(
select   bb.fym, aa.*
from dws_Report_DELL_Daily_WIP aa
inner join
(select fym,MAX(fdate)::date as fDate
from  public.dwd_dim_date_info bb
group by fym) bb on bb.fDate=updatetime::date
) bbb on (aaa.地区,aaa.种类,aaa.财年) = (bbb.AREA,bbb.Commodity,bbb.fym)
where d年.统计类型 in  ('WIP', 'OnWay', 'AWP', 'WIP<=5WD', 'WIP>5WD' )
and (d年.地区,d年.种类,d年.财年) = (bbb.AREA,bbb.Commodity,bbb.fym);

--更新当年度报表统计信息 --'AWP Rate'
insert into dwt_年度报表统计信息(地区, 种类, 财年, 统计类型, 数量)
   select aa.地区, aa.种类, aa.财年,'AWP Rate'  AS ftype,
          COALESCE(bb.数量,0)/ (aa.数量) as 数量
   from dwt_年度报表统计信息 aa
   left outer join dwt_年度报表统计信息 bb
        on aa.地区 = bb.地区 and aa.种类 = bb.种类  and bb.统计类型 = 'AWP'
       and aa.财年 = bb.财年
   where aa.统计类型 = 'WIP' and  aa.updatetime::date=(now() - interval '1 day')::date;

--更新当前年度报表信息 --'WIP<=5WD Rate'
insert into dwt_年度报表统计信息(地区, 种类, 财年, 统计类型, 数量)
 select aa.地区, aa.种类, aa.财年,
 'WIP<=5WD Rate' as 统计类型,COALESCE(bb.数量, 0) / aa.数量 as 数量
 --, aa.QTy as WIP, bb.QTy AS qty_5
 from dwt_年度报表统计信息 aa
 left outer join dwt_年度报表统计信息 bb
     on aa.地区 = bb.地区
            and aa.种类 = bb.种类
            and aa.财年 = bb.财年
            and bb.统计类型 = 'WIP<=5WD'
 where aa.统计类型 = 'WIP'
   and aa.数量 > 0
   and aa.updatetime::date=(now() - interval '1 day')::date;

select distinct 统计类型 from dwt_年度报表统计信息;
select * from dwt_年度报表统计信息 where 统计类型='Average TAT Total<=5wd'




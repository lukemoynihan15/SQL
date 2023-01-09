SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[pop_fact_sku_sales_by_day]   (@begin_date date, @end_date date ) as   

-- Commented out legacy Playa as last date loaded: 12/13/2021 -- Date:7/28/2022-- Shikhar
-- Playa has been removed
-- Adding in Wholesale Sell-in Data from NS

begin try

insert into mdata_reports..sp_logs(sp_name,sp_event,sp_status) SELECT OBJECT_NAME(@@PROCID),'Begin Procedure Execution','RUNNING'


declare @start_id int

select @start_id=max(id) from mdata_reports..sp_logs
where sp_name in (SELECT OBJECT_NAME(@@PROCID))
and sp_status='RUNNING'


if   @begin_date is null
Begin
	set @begin_date = dateadd(day, -30, getdate())
	set @end_date = dateadd(day, -1,convert(date, getdate()))

End


 delete fact_sku_sales_by_day
  where sales_date between @begin_date and @end_date
 
 ----LOAD ECOM

INSERT INTO [dbo].[fact_sku_sales_by_day] -- table
           ([sales_channel_id]
           ,[sales_date]
           ,[sku]
           ,[gross_sales_local]
           ,[gross_sales_converted_usd]
           ,[net_sales_converted_usd]
           ,[ordered_units]
           ,[net_units]
           ,[returned_units]
           ,[net_sales_local])

    select 
        case -- combines channel ID 16, 18 into 16, otherwise og
            when f.sales_channel_id in (16,18) 
                then 16 
            else f.sales_channel_id 
        end, 
        f.sales_date, 
        f.sku, 
        sum(f.gross_sales),
        -- us_conversion_rate is to getall currency into USD
        sum(f.gross_sales * c.us_conversion_rate) 
            as gross_sales, 
        sum(
            (isnull(f.gross_sales,0) + isnull(f.discounts,0) + isnull(f.returns,0))
                 * c.us_conversion_rate
            ) as net_sales,
        sum(f.ordered_units) 
            as ordered_units, 
        sum(f.net_units) 
            as net_units, 
        sum(f.returned_units) 
            as returned_units,
        sum(
            (isnull(f.gross_sales,0) + isnull(f.discounts,0) + isnull(f.returns,0))
            ) as net_sales_local -- no conversion rate, is local currency

    from fact_sales_by_order_sku f -- sales by order and sku. Basically a customer can buy more than one product in one order this would have mroe than 1 row
    inner join dim_sales_channel sc -- joined on channel
        on sc.id = f.sales_channel_id
    inner join dim_currency_conversion c 
        on c.currency_id = sc.currency_id
    where f.sales_date between @begin_date and @end_date -- user defined parameters
      and f.sales_date between c.begin_Date and isnull(c.end_Date, convert(date,getdate())) -- between begin date and end date or todays current date
    and sc.is_production = 1 -- currently being produced???
    and f.sku not in (21213,21214,21215) -- why excluding these???
    group by 
        case -- sales_channel_id
            when f.sales_channel_id in (16,18) 
                then 16 
            else f.sales_channel_id 
        end, 
        f.sales_date, 
        f.sku
   
---add retail

select * into #kwi_sales
	from kwi.dbo.vw_kwi_sales_by_transaction_sku v
	where   sales_date   between @begin_date and @end_date
			 
INSERT INTO [dbo].[fact_sku_sales_by_day]
           ([sales_channel_id]
           ,[sales_date]
           ,[sku]
           ,[gross_sales_local]
           ,[gross_sales_converted_usd]
           ,[net_sales_converted_usd]
		   ,[net_sales_local]
           ,[ordered_units]
           ,[net_units]
           ,[returned_units])

    select  s.id, 
			r.sales_date,
			p.sku ,
			sum(case when r.gross < 0 then 0 else r.gross end) gross,
			sum((case when r.gross < 0 then 0 else r.gross end) * c.us_conversion_rate) gross_usd,
			sum(net_sales * c.us_conversion_rate) net_sales,
			sum(net_sales) as net_sales_local,
			sum(ordered_units) ordered_units,
			sum(units) units,
			sum(returned_units) returned_units   
	from #kwi_sales r 
    inner join dim_sales_channel s 
	    on s.kwi_store_number = r.store_number  
	inner join dim_currency_conversion c 
        on c.currency_id = s.currency_id
	inner join kwi.dbo.articles a 
        on r.sku = a.article
		and r.country_id = a.country_id
	inner join mData.dbo.dim_products_sku_map pm
		on pm.sku = a.venitem
	inner join mdata.dbo.dim_products p 
        on p.sku = pm.netsuite_sku
		and a.country_id = r.country_id
	where s.type = 'Morphe Retail'
        and s.is_production = 1 
        and r.sales_date between c.begin_Date and isnull(c.end_Date, convert(date,getdate()))
    group by s.id, r.sales_date, p.sku
		
		 
-- Sellout data only -- from Wholesale Retailer Weekly Sales Process File Load
-- This is a user process owned by Shikhar
exec  pop_fact_sku_sales_by_day_whsl


---insert wholesale Sell-in data from NetSuite -- assumes that channels are mapped

INSERT INTO [dbo].[fact_sku_sales_by_day]
           ([sales_channel_id]
           ,[sales_date]
           ,[sku]
           ,[gross_sales_local]
           ,[gross_sales_converted_usd]
           ,[net_sales_converted_usd]
           ,[ordered_units]
           ,[net_units]
           ,[returned_units]
		   ) 
	

    select 
        sc.id as [sales_channel_id],
        convert(Date,f.trandate) [sales_date], 
        p.sku,
        null,
        sum(f.USD_converted) revenue ,
        sum(f.USD_converted) revenue ,
        sum( convert(bigint,f.units)) units ,
        sum( convert(bigint,f.units)) units ,
        null

    from mdata..vw_shipped_orders f 
    inner join mdata..dim_sales_channel sc 
        on sc.customer_id = f.customer_id
    inner join mdata..dim_products p 
        on p.net_suite_internal_id = f.item_id
    left outer join mdata..dim_forecast_group df 
        on df.id = sc.forecast_group_id
    left outer join mData_reports..dim_ns_classes c
        on p.class_id_new = c.class_id
    where sc.is_production =1
        and sc.selling_type = 'Sell-In'
        and sc.type = 'Wholesale NEW'
        and sc.id not in (7,10101,10137,150)
        and p.sku not in (10,10000,20854,20865,21213,21214,21215,21797,21798 )
        and c.class_id is not null
        and convert(Date,f.trandate) between @begin_date and @end_date
    group by  
        sc.id ,
        convert(Date,f.trandate),
        p.sku


			  
insert into mdata_reports..sp_logs(sp_name,sp_event,sp_status,start_id,source) 
SELECT OBJECT_NAME(@@PROCID),'End Procedure Execution','COMPLETED',@start_id,'SQL Server Agent'

end try

begin catch 

	insert into mdata_reports..sp_logs(sp_name,sp_event,sp_status,sp_error_message,start_id,source)
	SELECT OBJECT_NAME(@@PROCID),'End Procedure Execution','FAILED',ERROR_MESSAGE(),@start_id,'SQL Server Agent'

end catch
GO

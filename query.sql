-- ALTER SESSION SET WEEK_OF_YEAR_POLICY = 1;
-- ALTER SESSION SET week_start=7;
-- ALTER SESSION SET TIMEZONE = 'America/New_York';
use schema fulfillment_optimization_sandbox;
CREATE OR REPLACE TEMPORARY TABLE ib_damages AS
WITH t1 AS (
    SELECT DISTINCT
        ITEM,
        "QUANTITY_PER_CS"::int AS Eaches_per_Case,
        "QUANTITY_PER_LA"::int AS Eaches_per_Layer,
        "QUANTITY_PER_PA"::int AS Eaches_per_Pallet
    FROM EDLDB.chewybi.c_itemlocation_base_snapshot
    JOIN EDLDB.chewybi.products p 
        ON c_itemlocation_base_snapshot.ITEM = p.product_part_number
    JOIN EDLDB.chewybi.item_location_vendor i 
        ON i.product_part_number = c_itemlocation_base_snapshot.ITEM
        AND c_itemlocation_base_snapshot.snapshot_dt = i.snapshot_date
        AND c_itemlocation_base_snapshot.LOCATION = i.location_code
        AND c_itemlocation_base_snapshot.SUPPLIER = i.vendor_number
        AND c_itemlocation_base_snapshot.snapshot_dt = CURRENT_DATE
    WHERE product_vendor_location_disabled_flag = 'false'
      AND i.vendor_product_part_number IS NOT NULL
      AND LOCATION IN (
          SELECT DISTINCT wh_id 
          FROM EDLDB.fulfillment_analytics_sandbox.t_warehouses 
          WHERE type = 'FC'
      )
),
products AS (
    SELECT DISTINCT 
        wh_id,
        product_key,
        item_number,
        uom.length,
        uom.width,
        uom.height,
        product_manufacturer_name,
        parent_company,
        product_category_level3,
        product_merch_classification3,
        pa."attribute.packagingtype" AS packaging,
        p.product_name,
        uom_weight,
        product_unit_cost,
        Eaches_per_Case,
        Eaches_per_Layer,
        Eaches_per_Pallet
    FROM EDLDB.aad.t_item_uom uom
    JOIN EDLDB.chewybi.products p 
        ON uom.item_number = p.product_part_number
    JOIN EDLDB.chewybi.product_attributes pa 
        ON p.product_part_number::string = pa.partnumber::string
    LEFT JOIN t1 
        ON p.product_part_number = t1.ITEM
    WHERE wh_id IN (
        SELECT DISTINCT wh_id 
        FROM EDLDB.fulfillment_analytics_sandbox.t_warehouses 
        WHERE type = 'FC'
    )
),
vendors_with_rebates AS (
    SELECT DISTINCT supplier_site_number 
    FROM EDLDB.chewybi.market_medium_deals 
    WHERE dh_deal_start_date <= CURRENT_DATE 
      AND dh_deal_end_date > CURRENT_DATE 
      AND dh_deal_status = 'ACCEPTED'
),
base_dates AS (
    SELECT 
        product_part_number,
        MAX(document_order_dttm) AS max_date
    FROM EDLDB.chewybi.procurement_document_product_measures
    GROUP BY product_part_number
),
unit_costs AS (
    SELECT DISTINCT
        b.product_part_number,
        p.unit_cost,
        b.max_date
    FROM base_dates b
    JOIN EDLDB.chewybi.procurement_document_product_measures p 
        ON p.product_part_number = b.product_part_number 
       AND b.max_date = p.document_order_dttm
),
damage_info_1 AS (
    SELECT  
        t.wh_id,
        t.control_number AS po_number,
        c.financial_calendar_reporting_year,
        c.financial_calendar_reporting_period,
        start_tran_date,
        start_tran_date_time,
        CASE 
            WHEN t.start_tran_date < '2021-01-04' AND t.routing_code = '11' THEN 'Arrived Wet-unsaleable *'
            WHEN t.start_tran_date < '2021-01-04' AND t.routing_code = '12' THEN 'Infested/Pests *'
            WHEN t.start_tran_date < '2021-01-04' AND t.routing_code = '13' THEN 'DMG TRL/SKID'
            WHEN t.start_tran_date < '2021-01-04' AND t.routing_code = '14' THEN 'REWORK REQD'
            WHEN t.start_tran_date < '2021-01-04' AND t.routing_code = '15' THEN 'MISS/INCOMP'
            WHEN t.start_tran_date < '2021-01-04' AND t.routing_code = '16' THEN 'RIP/TEAR/DENT'
            WHEN t.start_tran_date < '2021-01-04' AND t.routing_code = '17' THEN 'EXPIRY/DATE'
            ELSE tr.description 
        END AS damage_type,
        t.item_number,
        SUM(t.tran_qty) AS total_units,
        SUM(t.tran_qty * pr.unit_cost) AS damage_cost
    FROM EDLDB.aad.t_tran_log t 
    LEFT JOIN unit_costs pr 
        ON t.item_number = pr.product_part_number
    LEFT JOIN EDLDB.fulfillment_analytics_sandbox.t_warehouses w 
        ON w.wh_id = t.wh_id
    LEFT JOIN EDLDB.aad.t_reason tr 
        ON tr.reason_id = t.routing_code
    LEFT JOIN products p 
        ON p.item_number = t.item_number 
        AND t.wh_id = p.wh_id
    LEFT JOIN EDLDB.chewybi.procurement_document_measures n 
        ON t.control_number = n.document_number
    LEFT JOIN EDLDB.chewybi.vendors v 
        ON n.vendor_key = v.vendor_key
    LEFT JOIN EDLDB.chewybi.common_date c 
        ON CAST(t.start_tran_date_time AS DATE) = c.common_date_dttm
    WHERE t.tran_type = '183' 
      AND n.document_ready_to_reconcile_dttm >= '2024-01-01' 
      AND t.control_number LIKE '%RS%' 
      AND w.type = 'FC' 
      AND tr.type = 'RECEIPT DAMAGED'
    GROUP BY 
        t.wh_id, t.control_number, c.financial_calendar_reporting_year,
        c.financial_calendar_reporting_period, start_tran_date,
        start_tran_date_time, t.routing_code, t.item_number, tr.description
)

SELECT 
    wh_id,
    financial_calendar_reporting_year,
    financial_calendar_reporting_period,
    start_tran_date,
    start_tran_date_time,
    po_number,
    item_number,
    SUM(total_units) AS total_units,
    SUM(damage_cost) AS total_damage_cost

FROM damage_info_1
WHERE start_tran_date_time >= current_timestamp - interval '24 hour' 
GROUP BY 
    wh_id,
    financial_calendar_reporting_year,
    financial_calendar_reporting_period,
    start_tran_date,
    start_tran_date_time,
    po_number,
    item_number
HAVING SUM(damage_cost) > 1000
ORDER BY start_tran_date_time DESC;


INSERT INTO EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K (
    wh_id,
    po_number,
    item_number,
    financial_calendar_reporting_year,
    financial_calendar_reporting_period,
    start_tran_date,
    start_tran_date_time,
    total_units,
    total_damage_cost,
    email_notified
)
SELECT 
    ib.wh_id,
    ib.po_number,
    ib.item_number,
    ib.financial_calendar_reporting_year,
    ib.financial_calendar_reporting_period,
    ib.start_tran_date,
    ib.start_tran_date_time,
    ib.total_units,
    ib.total_damage_cost,
    FALSE
FROM ib_damages ib
WHERE NOT EXISTS (
    SELECT 1 
    FROM EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K t
    WHERE t.wh_id = ib.wh_id
      AND t.po_number = ib.po_number
      AND t.item_number = ib.item_number
      AND t.start_tran_date_time = ib.start_tran_date_time
);
commit;
--select * from EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K
-- CREATE TABLE IF NOT EXISTS EDLDB.FULFILLMENT_OPTIMIZATION_SANDBOX.T_INBOUND_DAMAGES_OVER_1K (
--     wh_id STRING,
--     po_number STRING,
--     item_number STRING,
--     financial_calendar_reporting_year STRING,
--     financial_calendar_reporting_period STRING,
--     start_tran_date DATE,
--     start_tran_date_time TIMESTAMP,
--     total_units INT,
--     total_damage_cost FLOAT,
--     inserted_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
-- );

--frequency? as soon as a new entry in the table for notif.
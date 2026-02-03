# Shipment Analytics Data Dictionary

## Fact: dw.fact_shipments
- shipment_id (PK): Unique shipment identifier
- date_key (FK): Ship date key (YYYYMMDD)
- origin_port_key, dest_port_key (FK): Origin/Destination ports
- carrier_key, customer_key (FK): Carrier and customer
- planned_days: promised_delivery_date - ship_date
- actual_days: actual_delivery_date - ship_date
- delay_days: actual_days - planned_days
- on_time_flag: 1 if actual <= promised else 0
- weight_kg, cost_usd: shipment measures

## Dimensions
### dw.dim_date
Standard calendar attributes: month, quarter, year.

### dw.dim_port
Port reference: code, name, country, region.

### dw.dim_carrier
Carrier name and mode (Sea/Air/Rail/Ground).

### dw.dim_customer
Customer reference and segment.

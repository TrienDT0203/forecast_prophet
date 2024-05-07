# create table

table_schema_create = ('''
                       CREATE TABLE sqlite_schema(
                            type text,
                            name text,
                            tbl_name text,
                            rootpage integer,
                            sql text
                        )
                       '''
                       )

fct_fa_pnl_mini_create = (
                '''
                    CREATE TABLE IF NOT EXISTS fct_fa_pnl_mini (
                        complete_date date,
                        province text,
                        service_type text,
                        customer_type text,
                        partner text,
                        project text,
                        gsv_excluded_vat real,
                        stoppoint real,
                        aha_revenue real,
                        created_at datetime default current_timestamp
                    )
                '''
                )

oc_revenue_create = (
                '''
                    CREATE TABLE IF NOT EXISTS oc_revenue (
                        complete_date date,
                        team text,
                        sub_team text,
                        partner text,
                        province text,
                        postpaid integer,
                        invoice_type text,
                        project text,
                        service text,
                        service_id text,
                        service_type text,
                        stoppoint integer,
                        gsv real,
                        gsv_excluded_vat real,
                        aha_revenue real,
                        driver_fee real,
                        com_request_fee real,
                        no_com_request_fee real,
                        transit_fee real,
                        sms_fee real,
                        bulky_fee real,
                        promo_discount_value real,
                        promo_bd_discount real,
                        promo_subscription_discount real,
                        promo_mkt_discount real,
                        promo_internal_discount real,
                        promo_discount_order real,
                        diff_2pricing real,
                        other_earnings real,
                        diff real,
                        other_earnings_1 real,
                        balance_pay real,
                        total_pay real,
                        online_pay real,
                        hdbh real,
                        driver_income real,
                        vat_real real,
                        contract_discount real,
                        created_at datetime default current_timestamp
                    )
                '''
                )

database_operation_create = ('''
                CREATE TABLE IF NOT EXISTS database_operation (
                        id integer PRIMARY KEY,
                        action text,
                        action_status text,
                        date_perform date
                )            
            ''')

# drop table
fct_fa_pnl_mini_drop = "DROP TABLE IF EXISTS fct_fa_pnl_mini"
oc_revenue_drop = "DROP TABLE IF EXISTS oc_revenue"

# insert record
fct_fa_pnl_mini_insert = (
                '''
                    INSERT INTO fct_fa_pnl_mini (
                        complete_date, province, service_type, customer_type, partner, project, gsv_excluded_vat, stoppoint, aha_revenue 
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?,?
                    )
                '''
                )

oc_revenue_insert = (
                '''
                    INSERT INTO oc_revenue AS (
                        complete_date,team,sub_team,partner,province,postpaid,invoice_type,project,service,service_id,service_type,stoppoint,gsv,gsv_excluded_vat,aha_revenue,driver_fee,com_request_fee,no_com_request_fee,transit_fee,sms_fee,bulky_fee,promo_discount_value,promo_bd_discount,promo_subscription_discount,promo_mkt_discount,promo_internal_discount,promo_discount_order,diff_2pricing,other_earnings,diff,other_earnings_1,balance_pay,total_pay,online_pay,hdbh,driver_income,vat_real,contract_discount
                    )
                    VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    )
                '''
                )

database_operation_insert = (
                '''
                    INSERT INTO database_operation (
                        id, action, action_status, date_perform 
                    )
                    VALUES (
                        ?, ?, ?, ?
                    )
                '''
                )

# retrieve data
fct_fa_pnl_mini_query = (
                '''
                    SELECT * 
                    FROM fct_fa_pnl_mini 
                    WHERE true
                '''
                )

oc_revenue_query = (
                '''
                    SELECT * 
                    FROM oc_revenue 
                    WHERE true
                '''
                )

database_operation_query = (
                '''
                    SELECT * 
                    FROM database_operation 
                    WHERE true
                '''
                )

# Query list

drop_table_list = [fct_fa_pnl_mini_drop, oc_revenue_drop]

create_table_list = [fct_fa_pnl_mini_create, oc_revenue_create,database_operation_create]

insert_table_list = [fct_fa_pnl_mini_insert, oc_revenue_insert, database_operation_insert]

query_table_list = [fct_fa_pnl_mini_query, oc_revenue_query, database_operation_query]


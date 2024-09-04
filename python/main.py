import psycopg2
import json
import os
from dotenv import load_dotenv
import re
import datetime 

load_dotenv()


def get_postgres_cursor() -> psycopg2.connect:
    """ Function to create a connection into 
        Postgres with default values - in a non-test environment these
        credentials would be stored within AWS and retrieved at runtime """
    connection = psycopg2.connect(
        host = "localhost",
        database = os.environ["POSTGRES_DATABASE"],
        user = os.environ["POSTGRES_USER"],
        password = os.environ["POSTGRES_PASSWORD"],
        port = os.environ["PORT"]
    )

    return connection


def create_tables():
    # Refactor this
    category_table_dml = """
        create table categories(
            id                  TEXT PRIMARY KEY 
            , name              TEXT
        );
    """

    order_items_dml = """
        create table order_items(
            id                      TEXT PRIMARY KEY
            , vat_rate_takeaway    NUMERIC(10,2)
            , vat_rate_eatin        NUMERIC(10,2)
            , vat_amount            NUMERIC(10,2)
            , type                  TEXT
            , discount              NUMERIC(10,2)
            , name                  TEXT
            , total_amount          NUMERIC(10,2)
            , subtotal_amount       NUMERIC(10,2)
        );
    """

    kitchen_stations_dml = """
        create table kitchen_stations(
            id                      TEXT PRIMARY KEY
            , name                  TEXT
            , ext_tenant_uuid       TEXT
        );
    """

    orders_dml = """
        create table orders(
            id                    TEXT PRIMARY KEY
            , created_at            TIMESTAMP
            , ext__store_uuid       TEXT
            , ext_tenant_uuid       TEXT
            , delivery_fee          NUMERIC(10,2) DEFAULT NULL
            , discount              NUMERIC(10,2)
            , price                 NUMERIC(10,2)
            , service_charge        NUMERIC(10,2)
            , subtotal_amount       NUMERIC(10,2)
            , total_amount          NUMERIC(10,2)
            , vat_amount            NUMERIC(10,2)
            , requested_from         TEXT
            , status                TEXT
            , is_takeaway           BOOLEAN
            , timezone              TEXT
            , updated_at             TIMESTAMP
            , user_uuid             TEXT

            /*
            May run out of time to solve this
            , FOREIGN KEY (category_uuid) REFERENCES categories(id)
            ON DELETE SET NULL

            , FOREIGN KEY (order_item_uuid) REFERENCES order_items(id)
            ON DELETE SET NULL

            , FOREIGN KEY (kitchen_uuid) REFERENCES kitchen_stations(id)
            ON DELETE SET NULL*/
        );
    """

    cursor.execute(category_table_dml)
    cursor.execute(order_items_dml)
    cursor.execute(kitchen_stations_dml)
    cursor.execute(orders_dml)


def process_categories(data):
    # As we are creating the categories dimension we don't want to 
    # cause duplicates.
    # But first sense check we are passed a category JSON object
    # if "category" in data:
    sql_query = f"""
        merge into categories as target
        using (select '{data["uuid"]}' as id) as source
        on target.id = source.id
        when matched then
            update set 
                name = '{data["name"].replace("'","")}'

        when not matched then
            insert (id, name)
            values(
                '{data["uuid"]}'
                , '{data["name"].replace("'","")}'
            )
        ;
    """
    return sql_query
    # cursor.execute(sql_query)


def process_kitchen_stations(data):
    # Only want to run if we get this in JSON object
    # I'm making assumptions that this might not always be here
    # seems like it is, but I would have thought that with the categories   
    if data is not None: 
        sql_query = f"""
            merge into kitchen_stations as target
            using (select '{data["uuid"]}' as id) as source
            on target.id = source.id
            when matched then 
                update set
                    ext_tenant_uuid = '{data["extTenantUUID"]}' 
                    , name = '{data["name"]}'
            when not matched then
                insert (id, name, ext_tenant_uuid)
                values (
                    '{data["uuid"]}'
                    , '{data["name"].replace("'","")}'
                    , '{data["extTenantUUID"]}'
                )
            ;
        """
        return sql_query
    

def process_order_items(data):
    # Only want to run if we get this in JSON object
    # I'm making assumptions that this might not always be here
    # seems like it is, but I would have thought that with the categories
    if data["items"][0]["name"] is None: # REFACTOR
        name = ""
    else:
        name = data["items"][0]["name"].replace("'","")

    sql_query = f"""
        merge into order_items as target 
        using (select '{data["items"][0]["itemUUID"]}' as id) as source
        on target.id = source.id
        when matched then
            update set 
                vat_rate_takeaway = {data["items"][0]["vatRateTakeaway"]}
                , vat_rate_eatin = {data["items"][0]["vatRateEatIn"]}
                , vat_amount = {data["items"][0]["vatAmount"]}
                , type = '{data["items"][0]["type"]}' 
                , discount = '{data["items"][0]["discount"]}' 
                , name = '{name}' 
                , total_amount = {data["items"][0]["totalAmount"]}
                , subtotal_amount = {data["items"][0]["subtotalAmount"]}
            
        when not matched then 
            insert(
                id
                , vat_rate_takeaway
                , vat_rate_eatin
                , vat_amount
                , type
                , discount
                , name
                , total_amount
                , subtotal_amount
            )
            values(
                '{data["items"][0]["itemUUID"]}'
                , {data["items"][0]["vatRateTakeaway"]}
                , {data["items"][0]["vatRateEatIn"]}
                , {data["items"][0]["vatAmount"]}
                , '{data["items"][0]["type"]}' 
                , '{data["items"][0]["discount"]}' 
                , '{name}' 
                , {data["items"][0]["totalAmount"]}
                , {data["items"][0]["subtotalAmount"]}
            )
    """
    return sql_query
         

def process_orders(orders):
    # Refactor - I don't like this but running out of time
    if orders["payment"]["deliveryFee"] is None: 
         delivery_fee = 0
    else:
        delivery_fee = orders["payment"]["deliveryFee"]

    create_at_seconds = orders["createdAt"] / 1000
    update_at_seconds = orders["updatedAt"] / 1000
    sql_query = f"""
        merge into orders as target
        using (select '{orders["uuid"]}' as id) as source 
        on target.id = source.id
        when matched then 
            update set 
                ext__store_uuid = '{orders["extStoreUUID"]}'
                , ext_tenant_uuid = '{orders["extTenantUUID"]}'
                , delivery_fee = {delivery_fee}
                , discount = {orders["payment"]["discount"]}
                , price = {orders["payment"]["price"]}
                , service_charge = {orders["payment"]["serviceCharge"]}
                , subtotal_amount = {orders["payment"]["subtotalAmount"]}
                , total_amount = {orders["payment"]["totalAmount"]}
                , vat_amount = {orders["payment"]["vatAmount"]}
                , requested_from = '{orders["requestedFrom"]}'
                , status = '{orders["status"]}'
                , is_takeaway = {orders["takeaway"]}
                , timezone = '{orders["timezone"]}' 
                , updated_at = '{datetime.datetime.fromtimestamp(update_at_seconds).strftime('%Y-%m-%d %H:%M:%S')}'
                , user_uuid = '{orders["user"]["extUserUUID"]}'
                /*
                FIX THIS IF IF I HAVE TIME
                but essentially want to get a list of these ids
                , category_uuid = ''
                , order_item_uuid = '' 
                , kitchen_uuid = ''
                */
        when not matched then 
            insert(
                id
                , created_at
                , ext__store_uuid
                , ext_tenant_uuid 
                , delivery_fee 
                , discount
                , price
                , service_charge 
                , subtotal_amount 
                , total_amount 
                , vat_amount 
                , requested_from 
                , status 
                , is_takeaway
                , timezone 
                , updated_at 
                , user_uuid
            )
            values(
                '{orders["uuid"]}'
                , '{datetime.datetime.fromtimestamp(create_at_seconds).strftime('%Y-%m-%d %H:%M:%S')}'
                , '{orders["extStoreUUID"]}'
                , '{orders["extTenantUUID"]}'
                , {delivery_fee}
                , {orders["payment"]["discount"]}
                , {orders["payment"]["price"]}
                , {orders["payment"]["serviceCharge"]}
                , {orders["payment"]["subtotalAmount"]}
                , {orders["payment"]["totalAmount"]}
                , {orders["payment"]["vatAmount"]}
                , '{orders["requestedFrom"]}'
                , '{orders["status"]}'
                , {orders["takeaway"]}
                , '{orders["timezone"]}' 
                , '{datetime.datetime.fromtimestamp(update_at_seconds).strftime('%Y-%m-%d %H:%M:%S')}'
                , '{orders["user"]["extUserUUID"]}'
            )

    """
    return sql_query


def process_orders_nested_json(orders, cursor):
    for order in orders:
        print("Unnesting Category data")
        if "category" in order: 
            category_query = process_categories(order["category"])
            cursor.execute(category_query)

        print("Unnesting order Items")
        order_items_query = process_order_items(order["itemTypes"])
        cursor.execute(order_items_query)
        

        # TODO: Refactor Kitchen logic
        print("Unnesting Kitchen Station data")
        kitchen_station_query = process_kitchen_stations(order["kitchenStation"])
        if kitchen_station_query is None: 
            continue 
        else:
            cursor.execute(kitchen_station_query)

    return False
    

if __name__ == '__main__':
    print("Establishing connection to Postgres")
    postgres_conn = get_postgres_cursor()
    cursor = postgres_conn.cursor()
    print("Connection made!")

    # drop tables for debug only
    # cursor.execute("drop table if exists orders")
    # cursor.execute("drop table if exists categories")
    # cursor.execute("drop table if exists order_items")
    # cursor.execute("drop table if exists kitchen_stations")

    # Create tables and set up contraints
    print("Creating table structures to load data into")
    create_tables()
    print("Tables have been created")

    # Load JSON data from task_data.json
    print("Retrieving data from test_data.json")
    with open('./data/task_data.json', 'r') as file:
        data = json.load(file)
    print("File downloaded!")
    
    print("loading nested order data into DB")
    for order in data:
        process_orders_nested_json(order['bundles'], cursor)

        print("loading orders data into DB")
        orders_sql = process_orders(order)
        cursor.execute(orders_sql)

    print("Finished loading data into DB")


    # Debug only
    # cursor.execute("select * from orders;")
    # rows = cursor.fetchall()

    # for row in rows:
    #     print(row)

    # Commit all query executions so they persist in Postgres
    # and close all open connections
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()
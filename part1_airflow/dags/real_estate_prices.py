import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2025, 3, 18, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
    )
def prepare_prices_dataset():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, Float, String, Boolean, DateTime, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()

        metadata = MetaData()
        real_estate_prices = Table(
            'real_estate_prices',
            metadata,
            # flats
            Column('id', Integer, primary_key=True),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('total_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('studio', Boolean),
            Column('building_id', Integer),
            
            # buildings
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            
            # flats
            Column('price', BigInteger),

            UniqueConstraint('id', name='unique_flat_id')
            )

        # создавать, если таблица не существует
        if not inspect(db_conn).has_table(real_estate_prices.name): 
            metadata.create_all(db_conn)
            
    @task()
    def extract(**kwargs):

        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql_query = """
            SELECT -- flats.
                    flats.id,
                    flats.floor,
                    flats.kitchen_area,
                    flats.living_area,
                    flats.total_area,
                    flats.rooms,
                    flats.is_apartment,
                    flats.studio,
                    flats.building_id,
                    -- buildings.
                    buildings.build_year,
                    buildings.building_type_int,
                    buildings.latitude,
                    buildings.longitude,
                    buildings.ceiling_height,
                    buildings.flats_count,
                    buildings.floors_total,
                    buildings.has_elevator,
                    -- flats.
                    flats.price
            FROM flats
            LEFT JOIN buildings ON flats.building_id = buildings.id
            """
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        # TODO Пока непонятно, в чём должна заключаться трансформация данном случае?
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="real_estate_prices",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
            )

    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prepare_prices_dataset()
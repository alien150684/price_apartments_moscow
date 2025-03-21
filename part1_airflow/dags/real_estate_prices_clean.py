import pendulum
from airflow.decorators import dag, task
from steps.messages import send_telegram_success_message, send_telegram_failure_message

@dag(
    schedule='@weekly',
    start_date=pendulum.datetime(2025, 3, 7, tz="UTC"),
    catchup=False,
    tags=["ETL"],
    on_success_callback=send_telegram_success_message,
    on_failure_callback=send_telegram_failure_message
    )
def prepare_prices_dataset_clean():
    import pandas as pd
    import numpy as np
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    @task()
    def create_table():
        from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, Float, String, Boolean, DateTime, UniqueConstraint, inspect
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()

        metadata = MetaData()
        real_estate_prices_clean = Table(
            'real_estate_prices_clean',
            metadata,
            # flats
            Column('id', Integer, primary_key=True),
            Column('category_floor', String), # добавлен для моделирования
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
            Column('distance', Float), # добавлен для моделирования
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            
            # flats
            Column('price', BigInteger),

            UniqueConstraint('id', name='unique_flat_id_clean')
            )

        # создавать, если таблица не существует
        if not inspect(db_conn).has_table(real_estate_prices_clean.name): 
            metadata.create_all(db_conn)
            
    @task()
    def extract(**kwargs):
        import pandas as pd
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql_query = """
            SELECT *
            FROM real_estate_prices
            """
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data

    @task()
    def remove_duplicates(data: pd.DataFrame):
        """
        Функция удаления дубликатов
        """
        feature_cols = data.columns.drop(['id'])
        is_duplicated_features = data.duplicated(subset=feature_cols, keep='last')
        data = data[~is_duplicated_features] # .reset_index(drop=True)
        return data
    
    @task()
    def remove_outliers(data: pd.DataFrame):
        """
        Функция удаления редких и аномальных значений (выбросов)
        """
        data_clean = data[data['floor'].between(1, 30) &
                    data['floors_total'].between(1, 45) &
                    data['flats_count'].between(1, 1000) &
                    data['kitchen_area'].between(0, 40) &
                    data['living_area'].between(0, 150) &
                    data['total_area'].between(20, 250) &
                    data['ceiling_height'].between(2.5, 3.5) &
                    data['rooms'].between(1, 8) &
                    data['price'].between(1e6, 80e6) &
                    (data['build_year'] < pd.Timestamp.now().year)]
        return data_clean
    
    def category_floor(row: pd.Series):
        """
        Определяет категорию этажа: первый, последний, не крайний.
        """

        if row['floor'] == 1:
            return 'первый'
        elif row['floor'] == row['floors_total']:
            return 'последний'
        else:
            return 'не крайний'

    def calculate_distance_from_moscow_center(row: pd.Series):
        """
        Вычисляет расстояние от центра Москвы до заданной точки.

        Args:
            latitude (float): Широта объекта.
            longitude (float): Долгота объекта.

        Returns:
            float: Расстояние в километрах.
        """
        from geopy.distance import geodesic

        # Координаты центра Москвы (приблизительные)
        moscow_center = (55.7558, 37.6173) # Широта, Долгота

        # Координаты объекта
        object_coords = (row['latitude'], row['longitude'])

        # Вычисление расстояния с использованием geodesic (более точный метод)
        distance = geodesic(moscow_center, object_coords).km

        return distance
    
    @task()
    def add_features(data: pd.DataFrame):
        """
        Генерирует новые признаки
        """
        # Создадим новый столбец с категорией этажа.
        data.insert(loc=1, 
                    column='category_floor', 
                    value = data.apply(category_floor, axis=1))
        
        # Создадим новый столбец с расстоянием от центра Москвы.
        data.insert(loc=12, 
                    column='distance', 
                    value = data.apply(calculate_distance_from_moscow_center, axis=1))

        return data

    def area_round(area):
        """
        Округляет площади дляудобства восприятия и моделирования
        """
        if 0 <= area < 100:
            return round(area)
        elif area >= 100:
            return round(area, -1)
        else:
            return np.nan
    
    @task()
    def areas_round(data: pd.DataFrame):
        """
        Округляет площади для удобства восприятия и моделирования
        """
        for i in ('kitchen', 'living', 'total'):
            data.loc[:, f'{i}_area'] = data[f'{i}_area'].apply(area_round)
        
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="real_estate_prices_clean",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['id'],
            rows=data.values.tolist()
            )

    create_table()
    data = extract()
    data_without_duplicates = remove_duplicates(data)
    data_without_outliers = remove_outliers(data_without_duplicates)
    data_with_new_features = add_features(data_without_outliers)
    data_with_round_areas = areas_round(data_with_new_features)
    load(data_with_round_areas)
prepare_prices_dataset_clean()
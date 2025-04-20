# scripts/fit.py

import numpy as np
import pandas as pd
# from sklearn.compose import ColumnTransformer
# from sklearn.pipeline import Pipeline
# from category_encoders import CatBoostEncoder
# from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import cross_validate
from catboost import CatBoostRegressor
import yaml
import os
import json
import joblib

# обучение модели
def evaluate_and_fit_model():
	# Прочитайте файл с гиперпараметрами params.yaml
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)
	
    # Загрузите результат предыдущего шага: inital_data.csv
    data = pd.read_csv('data/initial_data.csv')
    data = data.drop(['id', 'building_id', # точно не повлияет на результат прогнозирования
                      'latitude', 'longitude', # могут "запутать модель"
                      'floor', 'flats_count','floors_total'], axis=1) # скорее всего, не значимы

    # Определение признаков и целевой переменной
    X = data.drop('price', axis=1)
    y = data['price']
   
    # Определение категориальных и числовых признаков
    categorical_features = X.select_dtypes(include=['object', 'boolean']).columns.tolist()
    categorical_features.append('building_type_int')

    numerical_features = X.select_dtypes(include=[np.number]).columns.tolist()
    numerical_features.remove('building_type_int')

    # Настройка модели CatBoost
    model = CatBoostRegressor(iterations=params['iterations'], 
                              learning_rate=params['learning_rate'], 
                              depth=params['depth'],
                              cat_features=categorical_features,
                              verbose=params['verbose'],
                              random_state=555)

    # Кросс-валидация
    cv_res = cross_validate(model, 
                            X, y, 
                            cv=params['n_splits'], 
                            scoring=params['metrics'],
                            n_jobs=params['n_jobs'], 
                            error_score=params['error_score'])

    # сохранение результата кросс-валидации в JSON
    cv_res_dict = dict()
    for key, value in cv_res.items():
        cv_res_dict[f'{key}'] = value.mean().round(2)
    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', "w") as f:
        json.dump(cv_res_dict, f, indent=4)  # indent=4 для красивого форматирования

    # сохранение результата кросс-валидации в JSON
    input_features_dict = dict()
    input_features_dict['cat_features'] = categorical_features
    input_features_dict['num_features'] = numerical_features
    with open('data/input_features.json', "w") as f:
        json.dump(input_features_dict, f, indent=4)  # indent=4 для красивого форматирования

    # Обучение модели на ВСЕХ данных ПОСЛЕ кросс-валидации
    model.fit(X, y)
    # сохранение модели в файл
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(model, fd)

if __name__ == '__main__':
	evaluate_and_fit_model()

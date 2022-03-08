import numpy as np
import os
import pandas as pd
import sqlalchemy
from sklearn.preprocessing import MinMaxScaler, OrdinalEncoder


class Dataset:

    def __init__(self):
        self.engine = sqlalchemy.create_engine(os.environ['DATABASE_URL_SQLA'])
        self.data: pd.DataFrame = pd.DataFrame()
        self.ex_data: pd.DataFrame = pd.DataFrame()
        self.volume_scaler = MinMaxScaler()
        self.expected_scaler = MinMaxScaler()
        self.rpe_scaler = MinMaxScaler()
        self.label_encoder = OrdinalEncoder()

    def get_data(self):
        query = '''Select set_id, w.user_id, date, "set".workout_id, set_number, et.name as exercise, 
                   reps, weight, unit_id, "set".rpe, es.reps_min, es.reps_max
                   from "set" join "workout" w on "set".workout_id = w.workout_id
                   join "expected_set" es on es.day_id = w.expected_id and es.exercise_id = "set".exercise_id and es.set_num = "set".set_number
                   join "exercise_type" et on "set".exercise_id = et.exercise_id'''

        with self.engine.connect() as connection:
            self.data = pd.read_sql(query, connection)
        self.ex_data = self.crete_exercise_data()

    def preprocess(self, user, exercise='all'):
        df = self.data[self.data['user_id'] == user].copy()
        if exercise != 'all':
            df = df[df['exercise'] == exercise].copy()
        df = df.groupby(['workout_id', 'exercise']).agg({'reps_min': 'sum',
                                                         'reps_max': 'sum',
                                                         'reps': 'sum',
                                                         'weight': 'sum',
                                                         'rpe': 'mean'})
        df['total_volume'] = df['reps'] * df['weight']
        df['expected_volume'] = ((df['reps_max'] + df['reps_min'])/2)*df['weight']
        df['scaled_volume'] = self.volume_scaler.fit_transform(df['total_volume'].to_numpy().reshape(-1, 1))
        df['scaled_expected'] = self.expected_scaler.fit_transform(df['expected_volume'].to_numpy().reshape(-1, 1))
        df['scaled_rpe'] = self.rpe_scaler.fit_transform(df['rpe'].to_numpy().reshape(-1, 1))
        df = df.sort_values(['workout_id'])
        df.reset_index(inplace=True)
        df['exercise'] = self.label_encoder.fit_transform(df['exercise'].to_numpy().reshape(-1, 1)).astype('int32')
        return df

    def crete_exercise_data(self):
        ex_df = self.data.groupby('exercise').agg({'weight': ['min', 'max']}).copy()
        ex_df.columns = ex_df.columns.droplevel(0)
        ex_df = ex_df.rename_axis(None, axis=1)
        ex_df.reset_index(inplace=True)
        return ex_df
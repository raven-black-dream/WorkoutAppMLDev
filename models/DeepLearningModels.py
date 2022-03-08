import numpy as np
import tensorflow as tf
from tensorflow.keras import Model
from tensorflow.keras.layers import Input, LSTM, Dense, Dropout, Embedding, Concatenate, Reshape


def build_lstm(input_length: int, input_width, exercise_count: int, output_length: int):
    if exercise_count != 1:
        output_dim = round(exercise_count/2)
    else:
        output_dim = 1
    inputs = Input(shape=(input_length, input_width), name='input')
    exercise_input = Input(shape=(input_length, 1), name='exercise_input')
    exercise_enc = Embedding(exercise_count, output_dim)(exercise_input)
    exercise_enc = Reshape((input_length, output_dim))(exercise_enc)
    merged = Concatenate()([inputs, exercise_enc])
    x = LSTM(64, return_sequences=True)(merged)
    x = LSTM(32, return_sequences=True)(x)
    x = Dropout(0.1)(x)
    x = LSTM(12)(x)
    outputs = Dense(output_length)(x)
    model = Model(inputs=[inputs, exercise_input], outputs=outputs)
    return model


def build_baseline(input_length: int, input_width, exercise_count: int, output_length: int):
    if exercise_count != 1:
        output_dim = round(exercise_count/2)
    else:
        output_dim = 1
    inputs = Input(shape=(input_length, input_width), name='input')
    exercise_input = Input(shape=(input_length, 1), name='exercise_input')
    exercise_enc = Embedding(exercise_count, output_dim)(exercise_input)
    exercise_enc = Reshape((input_length, output_dim))(exercise_enc)
    merged = Concatenate()([inputs, exercise_enc])
    x = Dense(128)(merged)
    x = Dense(64)(x)
    x = Dense(32)(x)
    outputs = Dense(output_length)(x)
    model = Model(inputs=[inputs, exercise_input], outputs=outputs)
    return model

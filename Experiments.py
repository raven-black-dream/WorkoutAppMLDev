import pandas as pd
import pprint
import models.DeepLearningModels as models
from Preprocessor import Dataset
from window_generator import WindowGenerator
import tensorflow as tf
import tensorflow.keras.losses as losses
import tensorflow.keras.metrics as metrics
from tensorflow.keras.optimizers import Adam


def run_experiment(model, name, df: pd.DataFrame, input_length: int, user, exercise):
    train_rows = round(df.shape[0] * 0.7)
    train_df = df[0:train_rows]
    test_df = df[train_rows: train_rows + round(df.shape[0] * 0.15)]
    val_df = df[train_rows + round(df.shape[0] * 0.15):]

    window = WindowGenerator(input_length, 1, 1, train_df, val_df, test_df, ['scaled_volume'])
    cp_dir = f'checkpoint/{name}/{user}/{exercise}/'
    checkpoint = tf.keras.callbacks.ModelCheckpoint(cp_dir,
                                                    monitor='val_loss',
                                                    verbose=1,
                                                    save_best_only=True,
                                                    save_weights_only=True)
    model.compile(loss=losses.MAPE,
                  optimizer=Adam(1e-4),
                  metrics=[metrics.MAPE])
    log_dir = f"logs/fit/{name}/{user}/{exercise}/"
    tensorboard_callback = tf.keras.callbacks.TensorBoard(log_dir=log_dir, histogram_freq=1)
    history = model.fit(window.train, epochs=10,
                        validation_data=window.val,
                        callbacks=[checkpoint, tensorboard_callback])
    model.load_weights(cp_dir)
    performance = model.evaluate(window.test, verbose=0)

    return history, performance


if __name__ == '__main__':

    gen = Dataset()
    gen.get_data()
    input_len = 2
    users = gen.data['user_id'].value_counts().to_frame()
    users = users[users['user_id'] >= 1000].copy()
    users = users.index.to_list()
    model_performance = {}
    for name in ('baseline', 'lstm'):
        model_performance[name] = []
        user_performance = {}
        for user in users:
            exercise_performance = {}
            exercises = gen.data['exercise'].value_counts().to_frame()
            exercises = exercises[exercises['exercise'] >= 75]
            for exercise in exercises.index.to_list():
                df = gen.preprocess(user, exercise)
                subset = df[['exercise', 'scaled_volume', 'scaled_expected', 'scaled_rpe']]
                n_exercise = len(df.exercise.unique())
                if name == 'baseline':
                    model = models.build_baseline(input_len, subset.shape[1] - 1, n_exercise, 1)
                else:
                    model = models.build_lstm(input_len, subset.shape[1] - 1, n_exercise, 1)
                history, exercise_performance[exercise] = run_experiment(model, name, subset, input_len, user, exercise)
            user_performance[user] = exercise_performance
            model_performance[name].append(user_performance)

    with open('Performance.txt', 'w') as f:
        pprint.pprint(model_performance, f)
    print('done!')


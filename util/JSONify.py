
import json
import os
import pandas as pd
import sqlalchemy


class DataGetter:

    def __init__(self):
        self.engine = sqlalchemy.create_engine(os.environ['DATABASE_URL_SQLA'])

    def process_data(self):
        with self.engine.connect() as conn:
            # Process User data
            # self.write_user_data(conn)
            # Process Exercise Data
            # self.write_exercise_data(conn)
            # Process Program Data
            # self.write_program_data(conn)
            # Process Program History Data
            # self.write_user_program_history(conn)
            # Process Workout History
            self.write_workout_data(conn)

    def write_user_data(self, conn):
        query = '''Select "user".user_id, first_name, last_name, program_name as current_program from "user"
                   join user_program on user_program.user_id = "user".user_id
                   join program on program.program_id = user_program.program_id
                   where user_program.current = 1 
                   order by "user".user_id '''
        results = [dict(result) for result in conn.execute(query).mappings().all()]
        with open('users.json', 'w') as f:
            json.dump(results, f, indent=4)
        return 0

    def write_exercise_data(self, conn):
        query = '''Select * from exercise_type order by exercise_id'''
        results = [dict(result) for result in conn.execute(query).mappings().all()]
        for result in results:
            result['weight_step'] = {'value': float(result['weight_step']), 'unit': 'lbs'}
        with open('exercises.json', 'w') as f:
            json.dump(results, f, indent=4)
        return 0

    def write_program_data(self, conn):
        query = '''Select program_id, program_name from "program"
                   '''
        programs = conn.execute(query).mappings().all()
        results = []
        for program in programs:
            program_data = {'program_name': program['program_name'], 'days': []}
            query = f'''Select program_day_id, day_name from "program_day"
                       where program_id = {program['program_id']} '''
            days = conn.execute(query).mappings().all()
            for day in days:
                day_data = {'day_name': day['day_name']}
                query = f'''Select *, et.name as exercise_name from "expected_set" 
                            join "exercise_type" et on et.exercise_id = "expected_set".exercise_id
                            where day_id = {day['program_day_id']}'''
                sets = conn.execute(query).mappings().all()
                df = pd.DataFrame.from_records(sets).sort_values('exp_set_id')
                df = df.groupby('exercise_id').agg({'exp_set_id': 'min',
                                                 'set_num': 'count',
                                                 'exercise_name': 'max',
                                                 'reps_min': 'max',
                                                 'reps_max': 'max',
                                                 'rpe': 'min'}).sort_values('exp_set_id')
                df['rpe'] = df['rpe'].astype('float32')
                day_data['sets'] = list(df[['set_num', 'exercise_name', 'reps_min', 'reps_max', 'rpe']].to_dict('records'))
                program_data['days'].append(day_data)
            results.append(program_data)
        with open('program.json', 'w') as f:
            json.dump(results, f, indent=4)
        return 0

    def write_workout_data(self, conn):
        query = '''Select workout_id, date, "user".first_name, "user".last_name, p.program_name, pd.day_name 
                   from "workout" w 
                   join "user" on "user".user_id = w.user_id
                   left join program_day pd on pd.program_day_id = w.expected_id
                   left join program p on p.program_id = pd.program_id'''
        workouts = conn.execute(query).mappings().all()
        data = []
        for workout in workouts:
            workout_data = {'date': workout['date'].strftime('%Y/%m/%d'),
                            'user': f"{workout['first_name']} {workout['last_name']}",
                            'program_name': workout['program_name'],
                            'program_day': workout['day_name']}
            query = f'''select set_id, name, reps, weight, rpe, set_number from "set" 
                       join exercise_type et on et.exercise_id = "set".exercise_id
                       where workout_id = {workout['workout_id']}
                       order by set_id'''
            sets = conn.execute(query).mappings().all()
            set_data = []
            for ex_set in sets:
                set_data.append({'exercise': ex_set['name'],
                                 'set_number': ex_set['set_number'],
                                 'reps': ex_set['reps'],
                                 'weight': {'value': float(ex_set['weight']), 'unit': 'lbs'},
                                 'rpe': float(ex_set['rpe'])})
            workout_data['sets'] = set_data
            data.append(workout_data)
        print('pause')
        with open('workout.json', 'w') as f:
            json.dump(data, f, indent=4)
        return 0


    def write_user_program_history(self, conn):
        query = '''Select "user".first_name, "user".last_name, program.program_name, current from user_program
                   join "user" on "user".user_id = user_program.user_id
                   join program on program.program_id = user_program.program_id 
                   order by user_program_id'''
        program_history = [{'user': f"{result['first_name']} {result['last_name']}",
                            'program_name': result['program_name'],
                            'current': result['current']} for result in conn.execute(query).mappings().all()]
        with open('program_history.json', 'w') as f:
            json.dump(program_history, f, indent=4)
        return 0


if __name__ == '__main__':
    gen = DataGetter()
    gen.process_data()

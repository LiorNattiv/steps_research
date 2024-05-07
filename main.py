import pandas as pd
import os
from datetime import timedelta
from dateutil.parser import parse, ParserError
import numpy as np
import yaml


def find_last_cycle_date(id_local, df_missing):
    try:
        last_cycle_date = parse(main_table[main_table['תעודת זהות '] == int(id_local)]['וסת אחרונה '].values[0])
    except IndexError:
        try:  # Trying with removal of last digit
            id_local = id_local[:-1]
            last_cycle_date = parse(main_table[main_table['תעודת זהות '] == int(id_local)]['וסת אחרונה '].values[0])
        except IndexError as e:
            print(e)
            df_missing.loc[len(df_missing)] = [id_local, None, 'תעודת זהות לא בטבלה ראשית']
            df_missing.to_csv("ids_diff.csv", encoding='utf-8-sig', index=False)
            last_cycle_date = None
    return last_cycle_date


def create_trimester_dict(df_local, last_cycle_date, birth_date, date_column):
    # Pre-Trimester
    tri_back = last_cycle_date - timedelta(days=90)
    df_pre_trimester = df_local[(pd.to_datetime(df_local[date_column], format='mixed') <= last_cycle_date) & (
                pd.to_datetime(df_local[date_column], format='mixed') >= tri_back)]

    # First Trimester
    start_of_first_tri = last_cycle_date + timedelta(days=1)
    end_of_first_tri = start_of_first_tri + timedelta(days=14 * 7)
    df_first_trimester = df_local[(pd.to_datetime(df_local[date_column], format='mixed') >= start_of_first_tri) & (
                pd.to_datetime(df_local[date_column], format='mixed') <= end_of_first_tri)]

    # Second Trimester
    start_of_second = end_of_first_tri + timedelta(days=1)
    end_of_second = start_of_second + timedelta(days=12 * 7)
    df_second_trimester = df_local[(pd.to_datetime(df_local[date_column], format='mixed') >= start_of_second) & (
                pd.to_datetime(df_local[date_column], format='mixed') <= end_of_second)]

    # Third Trimester
    start_of_third = end_of_second + timedelta(days=1)
    end_of_third = birth_date
    df_third_trimester = df_local[(pd.to_datetime(df_local[date_column], format='mixed') >= start_of_third) & (
                pd.to_datetime(df_local[date_column], format='mixed') <= end_of_third)]

    trimesters_dict = {'3 חודשים לפני ההריון': df_pre_trimester, 'טרימסטר ראשון': df_first_trimester,
                       'טרימסטר שני': df_second_trimester, 'טרימסטר שלישי': df_third_trimester}

    return trimesters_dict


def get_women_data(df_local, id_local):

    try:
        df_missing = pd.read_csv("ids_diff.csv")
    except FileNotFoundError:
        df_missing = pd.DataFrame(list(rep_dict.items()), columns=['file_id', 'main_table_id'])
        df_missing.loc[:, 'error'] = ''

    if 'Start' not in df_local.columns and 'Date' not in df_local.columns:
        print("No Start/Date column, skipping")
        df_missing.loc[len(df_missing)] = [id_local, None, "אין עמודת תאריך"]
        df_missing.to_csv("ids_diff.csv", encoding='utf-8-sig', index=False)

    elif len(df_local) == 0:
        print("No data for id, skipping")
        df_missing.loc[len(df_missing)] = [id_local, None, "אין נתונים בכלל בקובץ"]
        df_missing.to_csv("ids_diff.csv", encoding='utf-8-sig', index=False)

    else:
        if 'Start' in df_local.columns:
            date_column = 'Start'
            steps_column = 'Steps (count)'
        elif 'Date' in df_local.columns:
            date_column = 'Date'
            steps_column = 'Step Count (count)'
            if steps_column not in df_local.columns:
                steps_column = 'Step Count (steps)'

        id_local = id_local.split(".csv")[0]
        if id_local in list(rep_dict.keys()):
            id_local = rep_dict[id_local]
        missing_ids = df_missing[df_missing['main_table_id'] == None]['file_id'].values

        if id_local in missing_ids or int(id_local) in missing_ids:
            print(f"{id_local} is in df_missing file. skipping")
        else:

            last_cycle_date = find_last_cycle_date(id_local, df_missing)

            if last_cycle_date is not None:
                try:
                    birth_date = parse(main_table[main_table['תעודת זהות '] == int(id_local)]['תאריך גיוס'].values[0])
                except ParserError as e:
                    df_missing.loc[len(df_missing)] = [id_local, None, "אין תאריך גיוס בטבלה ראשית"]
                    df_missing.to_csv("ids_diff.csv", encoding='utf-8-sig', index=False)
                    birth_date = None

                if birth_date is not None:

                    trimesters_dict = create_trimester_dict(df_local, last_cycle_date, birth_date, date_column)

                    for trimester, df in trimesters_dict.items():

                        main_table.loc[main_table['תעודת זהות '] == int(id_local), f'חציון צעדים {trimester}'] = df[steps_column].median()
                        main_table.loc[main_table['תעודת זהות '] == int(id_local), f'ממוצע צעדים {trimester}'] = np.round(df[steps_column].mean(), 2)
                        main_table.loc[main_table['תעודת זהות '] == int(id_local), f'ס"ת צעדים {trimester}'] = np.round(df[steps_column].std(), 2)

                    cols_to_fill = config['cols_to_fill']
                    mask = main_table['תעודת זהות '] == int(id_local)
                    main_table.loc[mask, cols_to_fill] = main_table.loc[mask, cols_to_fill].fillna('N/A')

                    if not test:
                        main_table.to_csv("final_table.csv", encoding='utf-8-sig', index=False)


if __name__ == '__main__':

    all_ids = [x for x in os.listdir('data') if x != 'mere naail.csv']

    main_table = pd.read_excel("טבלה מלאה ערוכה- רבקה - 16.3.24.xlsx", header=1)
    main_table = main_table[main_table['תעודת זהות '].notna()]
    main_table['תעודת זהות '] = main_table['תעודת זהות '].astype(int)

    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    rep_dict = config['rep_dict']

    test = False

    for i, id in enumerate(all_ids):
        print(i, id)
        df_global = pd.read_csv(f"data/{id}")
        get_women_data(df_global, id)

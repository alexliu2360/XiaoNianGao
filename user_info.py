# coding='utf8'
# import numpy as np
import pandas as pd


data_path = '../data/'
listing_info_fn = 'listing_info.csv'
test_fn = 'test.csv'
train_fn = 'train.csv'
user_behavior_logs_fn = 'user_behavior_logs.csv'
user_info_fn = 'user_info.csv'
user_repay_logs_fn = 'user_repay_logs.csv'
user_taglist_fn = 'user_taglist.csv'

preprocess_listing_info_fn = 'preprocess_listing_info.csv'
preprocess_test_fn = 'preprocess_test.csv'
preprocess_train_fn = 'preprocess_train.csv'
preprocess_user_behavior_logs_fn = 'preprocess_user_behavior_logs.csv'
preprocess_user_info_fn = 'preprocess_user_info.csv'
preprocess_user_repay_logs_fn = 'preprocess_user_repay_logs.csv'
preprocess_user_taglist_fn = 'preprocess_user_taglist.csv'

preprocess_train_data_listing_info_fn = 'preprocess_train_data_listing_info.csv'
user_info = pd.read_csv(data_path + preprocess_user_info_fn, encoding='utf-8')
user_info = pd.concat([user_info, pd.DataFrame(columns=['insertdata_first', 'insertdata_last', 'insertdata_times'])],
                      sort=True)
user_info_copy = user_info.copy(deep=True)

handled_index_list = []
count = 0
join_index = ['index', 'insertdata_first', 'insertdata_last', 'insertdata_times']
join_serise = pd.Series(index=join_index)
for idx, row in user_info.iterrows():
    count += 1
    if count % 1000 == 0:
        print(count)
    #         if count == 500:
    #             break
    try:
        temp_user = user_info_copy.loc[user_info_copy['user_id'] == row['user_id']]
        temp_user_insterdata_times = len(temp_user)
        if temp_user_insterdata_times == 0:
            print('error')
            break
        if temp_user_insterdata_times == 1:
            temp_series = pd.Series([idx, temp_user['insert_ts'].values[0], temp_user['insert_ts'].values[0], 1],
                                    index=join_index)
            join_serise = pd.concat([join_serise, temp_series], axis=1)
            continue
        if idx in handled_index_list:
            continue
        else:
            handled_index_list.append(idx)
        temp_user_ts = temp_user['insert_ts']
        temp_user_ts_max = temp_user_ts.max()
        temp_user_ts_min = temp_user_ts.min()
        temp_user_ts_dict = {v: k for k, v in temp_user_ts.to_dict().items()}
        temp_user_ts_max_idx = temp_user_ts_dict[temp_user_ts_max]
        temp_user_index_list = temp_user.index.to_list()
        temp_user_index_list.remove(temp_user_ts_max_idx)
        user_info_copy.drop(index=temp_user_index_list, axis=0, inplace=True)
        temp_series = pd.Series([temp_user_ts_max_idx, temp_user_ts_min, temp_user_ts_max, temp_user_insterdata_times],
                                index=join_index)
        join_serise = pd.concat([join_serise, temp_series], axis=1)
    except:
        print(row['user_id'])
        break

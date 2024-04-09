# -*- coding: utf-8 -*-
# @Time    : 2024/3/31 16:43
# @Author  : WTY
# @FileName: length_cut_position.py
# @Software: PyCharm
import numpy as np

from ibadatfile import IbaDatFile
from pathlib import Path
import glob
import pandas as pd

if __name__ == '__main__':

    # 初始化数据文件列表
    folder_path = './data2/'
    pda_data_path = glob.glob(folder_path + "/bao*.dat")
    # 调用接口读取第一个数据文件
    with IbaDatFile(pda_data_path[0]) as file:
        main_df = file.data()
        # 找到第一行中值为 True 或 False 的列 (删除模拟量)
        columns_to_drop = main_df.columns[main_df.iloc[0].isin([True, False])]
        main_df.drop(columns=columns_to_drop, inplace=True)

    # 循环读取剩余数据文件
    for i in range(1, len(pda_data_path)):
        with IbaDatFile(pda_data_path[i]) as file:
            temp_df = file.data()
            temp_df.drop(columns=columns_to_drop, inplace=True)
            main_df = pd.concat([main_df, temp_df])
    # 提取长度字段数据并计算相邻数据帧的长度差分
    length_df = main_df['ACTUAL STRIP LENGTH']
    length_diff_df = main_df['ACTUAL STRIP LENGTH'].diff()

    # 长度突变判断阈值
    cut_condition = length_diff_df < -1000

    # 创建分割位置列表
    cut_bit = np.where(cut_condition)[0]
    cut_list = cut_bit.tolist()

    # 初始化输出路径
    output_path = './processed/'
    # 初始化分割文件名序号
    start_index = 1
    # 根据分割列表导出保存数据
    for i, end_index in enumerate(cut_list, start=1):
        cut_df = main_df.iloc[start_index:end_index]
        filename_index = str(i).zfill(3)
        print(f'正在生成卷{filename_index}.csv')
        cut_df.to_csv(f'{output_path}卷{filename_index}.csv', index=False)
        start_index = end_index

    # 保存末端数据
    print(f'正在生成卷{str(len(cut_list) + 1).zfill(3)}.csv')
    main_df.iloc[start_index:].to_csv(f'{output_path}卷{str(len(cut_list) + 1).zfill(3)}.csv', index=False)
    print(cut_list)
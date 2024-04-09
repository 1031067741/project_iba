# -*- coding: utf-8 -*-
# @Time    : 2024/3/31 16:43
# @Author  : WTY
# @FileName: length_cut_position.py
# @Software: PyCharm
import numpy as np

from ibadatfile import IbaDatFile
# from pathlib import Path
import glob
import pandas as pd
import gc

if __name__ == '__main__':
    # 降采样标志：flag=1表示启用降采样；speed表示采样速率
    down_sample_flag = 0
    down_sample_speed = 10

    # 读取步长：不建议过大，防止内存不足
    read_step = 2
    # 记录文件读取序号
    read_start = 1
    read_end = read_start + read_step

    # 记录卷号
    coil_index = 1

    # 初始化输出路径
    output_path = './processed/'

    # 钢卷长度突变判断阈值
    length_jump = -1000

    # 初始化数据文件列表
    folder_path = './data2/'
    pda_data_path = glob.glob(folder_path + "/bao*.dat")
    datafile_len = len(pda_data_path)

    # 调用接口读取第一个数据文件
    with IbaDatFile(pda_data_path[0]) as file:
        main_df = file.data()
    # 找到第一行中值为 True 或 False 的列 (删除模拟量)
    columns_to_drop = main_df.columns[main_df.iloc[0].isin([True, False])]
    main_df.drop(columns=columns_to_drop, inplace=True)

    # 清理内存
    del file
    gc.collect()

    while read_end <= datafile_len:
        if read_end + read_step > datafile_len:
            read_end = datafile_len
        # 循环读取剩余数据文件
        for i in range(read_start, read_end):
            with IbaDatFile(pda_data_path[i]) as file:
                temp_df = file.data()
                del file
            try:
                temp_df.drop(columns=columns_to_drop, inplace=True)
                main_df = pd.concat([main_df, temp_df])
                del temp_df
                gc.collect()
            except Exception as e:
                print("发生错误：", e)

        # 下一轮的文件读取序号
        read_start = read_end + 1
        read_end = read_start + read_step

        # 提取长度字段数据并计算相邻数据帧的钢卷长度差分
        length_df = main_df['ACTUAL STRIP LENGTH']
        length_diff_df = main_df['ACTUAL STRIP LENGTH'].diff()

        # 根据突变阈值寻找分割点序列
        cut_condition = length_diff_df < length_jump

        # 创建分割位置列表
        cut_bit = np.where(cut_condition)[0]
        cut_list = cut_bit.tolist()

        # 初始化分割序号
        start_index = 1
        print(cut_list)

        # 根据分割列表导出保存数据
        if cut_list is not None:
            for i, end_index in enumerate(cut_list, start=1):
                cut_df = main_df.iloc[start_index:end_index]
                filename_index = str(coil_index).zfill(3)
                coil_index = coil_index + 1
                print(f'正在生成卷{filename_index}.csv')
                if down_sample_flag == 1:
                    # 以10的速率进行采样
                    cut_df = cut_df[::down_sample_speed]
                    cut_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{filename_index}.csv', index=False)
                else:
                    cut_df.to_csv(f'{output_path}卷{filename_index}.csv', index=False)
                print(f'已生成卷{filename_index}.csv')
                start_index = end_index
        else:
            # 如果本轮没有找到分割点，减少下一轮文件读取数量，防止内存不足
            read_end = read_start

        # 保存本轮的末端数据进行下一轮拼接
        main_df = main_df.iloc[start_index:]

        # 内存清理
        try:
            del length_df
            del length_diff_df
            del cut_list
            del cut_bit
            del cut_condition
            del cut_df
        except Exception as e:
            print("本轮没有找到分割点：", e)
        gc.collect()

    # 已读取全部文件，保存剩余数据
    print(f'正在生成卷{str(coil_index).zfill(3)}.csv')
    if down_sample_flag == 1:
        # 以10的速率进行采样
        main_df = main_df[::10]
        main_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{str(coil_index).zfill(3)}.csv', index=False)
    else:
        main_df.to_csv(f'{output_path}卷{str(coil_index).zfill(3)}.csv', index=False)
    print(f'已生成卷{str(coil_index).zfill(3)}.csv')
    print('全部数据分割完成，程序退出')
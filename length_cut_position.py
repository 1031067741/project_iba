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
    down_sample_flag = True
    down_sample_speed = 10

    # 读取步长：不建议过大，防止内存不足
    read_step = 3
    # 记录文件读取序号
    read_start = 1
    read_end = read_start + read_step
    # 记录单卷数据文件数量
    coil_file_num = 1
    # 记录内存不足前能够允许的最大数据文件数量
    coil_file_num_max = 6
    # 记录卷号
    coil_index = 1
    # 记录单卷的分卷号
    coil_index_part = 1

    produce_stop_index = 1
    produce_stop_flag = False

    # 初始化输出路径
    output_path = './processed/'

    # 钢卷长度突变判断阈值
    length_jump = -1000

    # 初始化数据文件列表
    folder_path = 'G:/work/180718/'
    pda_data_path = glob.glob(folder_path + "bao*.dat")
    datafile_len = len(pda_data_path)

    print('读取', pda_data_path[0])
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
            print('读取',pda_data_path[i])
            with IbaDatFile(pda_data_path[i]) as file:
                temp_df = file.data()
                del file
            try:
                temp_df.drop(columns=columns_to_drop, inplace=True)
                main_df = pd.concat([main_df, temp_df],ignore_index=True)
                del temp_df
                gc.collect()
            except Exception as e:
                print("发生错误：", e)

        # 下一轮的文件读取序号
        read_start = read_end
        read_end = read_start + read_step

        # 提取长度字段数据并计算相邻数据帧的钢卷长度差分
        length_df = main_df['ACTUAL STRIP LENGTH']
        length_diff_df = main_df['ACTUAL STRIP LENGTH'].diff()

        # 根据突变阈值寻找分割点序列
        cut_condition = length_diff_df < length_jump

        # 创建分割位置列表
        cut_bit = np.where(cut_condition)[0]
        cut_list = cut_bit.tolist()

        # 标记连续相同值的段
        groups = (length_df.shift() != length_df).cumsum()

        # 如果长度连续停滞，修改保存的文件名序号
        if produce_stop_flag:
            produce_stop_index = produce_stop_index + 1
        else:
            produce_stop_index = 1

        # 判断长度是否长时间不变化
        produce_stop_flag = any(length_df.groupby(groups).apply(lambda x: len(x) >= 24000))

        # 长度长时间不变化
        if produce_stop_flag:
            # 计算每个组的大小
            group_sizes = length_df.groupby(groups).size()
            # 筛选出大小大于或等于24000的组
            large_groups = group_sizes[group_sizes >= 24000]
            # 获取这些组的起始和结束索引
            produce_stop_start_end_indices = length_df.groupby(groups).apply(lambda x: (x.index[0], x.index[-1])).loc[
                large_groups.index]
            # 将起始和结束索引存储在变量中
            product_stop_start, product_stop_end = produce_stop_start_end_indices.iloc[0]
            filename_index = str(coil_index).zfill(3)
            produce_stop_cut_df = main_df.iloc[:product_stop_end]
            if coil_index_part > 1:
                if down_sample_flag:
                    # 以down_sample_speed的速率进行降采样
                    produce_stop_cut_df = produce_stop_cut_df[::down_sample_speed]
                    print(f'正在生成：(降采样{down_sample_speed}x)卷{filename_index}part{coil_index_part}中停机part{produce_stop_index}')
                    produce_stop_cut_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{filename_index}part{coil_index_part}中停机part{produce_stop_index}.csv',index=False)
                    print(f'已生成：(降采样{down_sample_speed}x)卷{filename_index}part{coil_index_part}中停机part{produce_stop_index}')
                else:
                    print(f'正在生成：卷{filename_index}part{coil_index_part}中停机part{produce_stop_index}')
                    produce_stop_cut_df.to_csv(f'{output_path}卷{filename_index}part{coil_index_part}中停机part{produce_stop_index}.csv',index=False)
                    print(f'已生成：卷{filename_index}part{coil_index_part}中停机part{produce_stop_index}')
            else:
                if down_sample_flag:
                    # 以down_sample_speed的速率进行降采样
                    produce_stop_cut_df = produce_stop_cut_df[::down_sample_speed]
                    print(f'正在生成：(降采样{down_sample_speed}x)卷{filename_index}前停机part{produce_stop_index}')
                    produce_stop_cut_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{filename_index}前停机part{produce_stop_index}.csv',index=False)
                    print(f'已生成：(降采样{down_sample_speed}x)卷{filename_index}前停机part{produce_stop_index}')
                else:
                    print(f'正在生成：卷{filename_index}前停机part{produce_stop_index}')
                    produce_stop_cut_df.to_csv(f'{output_path}卷{filename_index}前停机part{produce_stop_index}.csv', index=False)
                    print(f'已生成：卷{filename_index}前停机part{produce_stop_index}')

            main_df = main_df.iloc[product_stop_end:]
            # 重置索引并丢弃旧的索引列
            main_df = main_df.reset_index(drop=True)

            # 清理内存
            del produce_stop_cut_df
            del groups
            del large_groups
            del group_sizes
            del produce_stop_start_end_indices
            del product_stop_start
            del product_stop_end
            gc.collect()

        # 生产正常进行
        else:
            # 如果本轮找到分割点
            if len(cut_list) > 0 :
                # 初始化分割序号
                start_index = 1
                print('本轮分割点索引：', cut_list)
                # 重置本卷数据文件数量计数
                coil_file_num = 1
                # 根据分割列表导出保存数据
                for i, end_index in enumerate(cut_list, start=1):
                    cut_df = main_df.iloc[start_index:end_index]
                    filename_index = str(coil_index).zfill(3)
                    coil_index = coil_index + 1

                    if down_sample_flag:
                        # 以down_sample_speed的速率进行降采样
                        cut_df = cut_df[::down_sample_speed]
                        if coil_index_part > 1:
                            print(f'正在生成：(降采样{down_sample_speed}x)卷{filename_index}part{coil_index_part}')
                            cut_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{filename_index}part{coil_index_part}.csv',index=False)
                            print(f'已生成：  (降采样{down_sample_speed}x)卷{filename_index}part{coil_index_part}')
                            coil_index_part = 1
                        else:
                            print(f'正在生成：(降采样{down_sample_speed}x)卷{filename_index}')
                            cut_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{filename_index}.csv', index=False)
                            print(f'已生成：  (降采样{down_sample_speed}x)卷{filename_index}')
                    else:
                        if coil_index_part > 1:
                            print(f'正在生成：卷{filename_index}part{coil_index_part}')
                            cut_df.to_csv(f'{output_path}卷{filename_index}part{coil_index_part}.csv', index=False)
                            print(f'已生成：  卷{filename_index}part{coil_index_part}')
                            coil_index_part = 1
                        else:
                            print(f'正在生成：卷{filename_index}')
                            cut_df.to_csv(f'{output_path}卷{filename_index}.csv', index=False)
                            print(f'已生成：  卷{filename_index}')
                    start_index = end_index

                    coil_file_num = 1

                    # 保存本轮的末端数据进行下一轮拼接
                    main_df = main_df.iloc[start_index:]
                    # 重置索引并丢弃旧的索引列
                    main_df = main_df.reset_index(drop=True)

            # 如果本轮没有找到分割点
            else:
                # 减少下一轮文件读取数量，防止内存不足
                read_end = read_start + 1
                print(read_start ,read_end)
                # 记录本卷生产数据的文件数量
                coil_file_num = coil_file_num + 1
                if coil_file_num >= coil_file_num_max:
                    print(f'正在生成：卷{str(coil_index).zfill(3)}part{coil_index_part}')
                    if down_sample_flag:
                        # 以down_sample_speed的速率进行降采样
                        main_df = main_df[::down_sample_speed]
                        print(f'正在生成：(降采样{down_sample_speed}x)卷{str(coil_index).zfill(3)}part{coil_index_part}')
                        main_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{str(coil_index).zfill(3)}part{coil_index_part}.csv',
                                       index=False)
                        print(f'已生成：  (降采样{down_sample_speed}x)卷{str(coil_index).zfill(3)}part{coil_index_part}')
                    else:
                        print(f'正在生成：卷{str(coil_index).zfill(3)}part{coil_index_part}')
                        main_df.to_csv(f'{output_path}卷{str(coil_index).zfill(3)}part{coil_index_part}.csv', index=False)
                        print(f'已生成：  卷{str(coil_index).zfill(3)}part{coil_index_part}')

                    coil_index_part = coil_index_part + 1

                    # 清空dataframe
                    main_df.drop(main_df.index, inplace=True)
                    gc.collect()

                    # 重置本卷数据文件数量计数
                    coil_file_num = 1

        print('长时间停机：',produce_stop_flag)
        print('------------------------------------')

        # 内存清理
        try:
            del length_df
            del length_diff_df
            del cut_list
            del cut_bit
            del cut_condition
            del cut_df
            del groups
        except Exception as e:
            print("本轮没有找到分割点：", e)
        gc.collect()

    # 已读取全部文件，保存剩余数据

    if down_sample_flag:
        # 以down_sample_speed的速率进行降采样
        main_df = main_df[::down_sample_speed]
        print(f'正在生成：(降采样{down_sample_speed}x)卷{str(coil_index).zfill(3)}')
        main_df.to_csv(f'{output_path}(降采样{down_sample_speed}x)卷{str(coil_index).zfill(3)}.csv', index=False)
        print(f'已生成：  (降采样{down_sample_speed}x)卷{str(coil_index).zfill(3)}')
    else:
        print(f'正在生成：卷{str(coil_index).zfill(3)}')
        main_df.to_csv(f'{output_path}卷{str(coil_index).zfill(3)}.csv', index=False)
        print(f'已生成：  卷{str(coil_index).zfill(3)}')
    print('全部数据分割完成，程序退出')
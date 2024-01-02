import asyncio

import pandas as pd

from Selenium import Crawling, get_data

file_list = []


async def run():
    data = get_data()
    print('load data complete')
    chunk_size = 2200
    tasks = []
    for i in range(0, len(data), chunk_size):
        chunk = data.iloc[i:i + chunk_size]
        random_num = 49000 + (i // chunk_size)
        worker = Crawling(chunk, random_num)
        task = asyncio.create_task(worker.run())
        print(i // chunk_size, 'task created')
        file_list.append(f'../data/{random_num}.csv')
        tasks.append(task)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    print('###end###')

    dfs = []
    for f in file_list:
        df = pd.read_csv(f)
        dfs.append(df)
    result_df = pd.concat(dfs, ignore_index=True)
    result_df.to_csv('../data/merged_data.csv', index=False, encoding='utf-8')

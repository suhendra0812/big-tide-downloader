import asyncio
from datetime import datetime
import httpx
import pandas as pd
import numpy as np

def tide_intepolation(df: pd.DataFrame, date_time: datetime, datetime_column: str = 'datetime') -> float:
    df.set_index(datetime_column, inplace=True)
    index_list = df.index.tolist().append(date_time)
    df = df.reindex(index_list)
    interp_level = df['level'].interpolate(method='time')[-1]

    return interp_level

async def tide_prediction(client: httpx.Client, lon: float, lat: float, date_time: datetime) -> float:
    url = 'https://srgi.big.go.id/tides_data/prediction-v2'

    params = {
        'coords': f'{lon},{lat}',
        'awal': f'{date_time.date().isoformat()}',
        'akhir': f'{date_time.date().isoformat()}'
    }
    
    r = await client.get(url, params=params)

    results = r.json()['results']
    predictions = results['predictions']
    ids = [i for i in predictions.keys()]
    values = [v for v in predictions.values()]
    df = pd.DataFrame(data=values, index=pd.Index(ids))
    df.columns = ["lat", "lon", "date", "time", "level"]
    df['lat'] = df['lat'].astype(float)
    df['lon'] = df['lon'].astype(float)
    df['level'] = df['level'].astype(float)
    df["datetime"] = pd.to_datetime(df["date"].str.cat(df["time"], sep='T'), utc=True)

    interp_level = tide_intepolation(df, date_time)

    return interp_level

async def main() -> None:
    lon, lat = 113.7162,-7.5433
    date_range = pd.date_range("2015-01-01", "2022-08-12", freq="2M")
    
    async with httpx.AsyncClient(timeout=30) as client:

        tasks = []
        for date in date_range:
            task = asyncio.ensure_future(tide_prediction(client, lon, lat, date))
            tasks.append(task)

        levels = await asyncio.gather(*tasks)
        lons = np.repeat(lon, len(levels))
        lats = np.repeat(lat, len(levels))

        df = pd.DataFrame({'lat': lats, 'lon': lons, 'datetime': date_range, 'level': levels})
        print(df)

if __name__ == '__main__':
    asyncio.run(main())

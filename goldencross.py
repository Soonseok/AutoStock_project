from pykrx import stock
import numpy as np
import pandas as pd
from pandas import Series
from time import sleep


def mam_earning_rate(df, Ns, Nl):
    df = df[['종가']].copy()
    df['ma_s'] = df['종가'].rolling(Ns).mean().shift(1)
    df['ma_l'] = df['종가'].rolling(Nl).mean().shift(1)
    cond = (df['ma_s'] > df['ma_l']) & (df['ma_l'].pct_change() > 0)
    df['status'] = np.where(cond, 1, 0)
    df.iloc[-1, -1] = 0

    # 매수/매도 조건
    매수조건 = (df['status'] == 1) & (df['status'].shift(1) != 1)
    매도조건 = (df['status'] == 0) & (df['status'].shift(1) == 1)

    # 수익률 계산
    수익률 = df.loc[매도조건, '종가'].reset_index(
        drop=True) / df.loc[매수조건, '종가'].reset_index(drop=True)
    수익률 = 수익률 - 1.002
    return 수익률.cumprod().iloc[-1]


def cal_mam_of_tickers(n):
    results = []
    tickers = stock.get_index_portfolio_deposit_file(f"{n}")
    for ticker in tickers:
        df = stock.get_market_ohlcv_by_date("20200101", "20210527", ticker)
        df = df[['종가']]
        result = []
        for i in range(2, 17):
            for j in range(30, 61):
                result.append(mam_earning_rate(df, i, j))
        index = pd.MultiIndex.from_product([range(2, 17), range(30, 61)])
        s = Series(result, index)
        results.append([ticker, s.max(), s.idxmax()])
        sleep(1)
    return results


'''
1001 코스피
1028 코스피 200
1034 코스피 100
1035 코스피 50
1167 코스피 200 중소형주
1182 코스피 200 초대형제외 지수
1244 코스피200제외 코스피지수
1150 코스피 200 커뮤니케이션서비스
1151 코스피 200 건설
1152 코스피 200 중공업
1153 코스피 200 철강/소재
1154 코스피 200 에너지/화학
1155 코스피 200 정보기술
1156 코스피 200 금융
1157 코스피 200 생활소비재
1158 코스피 200 경기소비재
1159 코스피 200 산업재
1160 코스피 200 헬스케어
1005 음식료품
1006 섬유의복
1007 종이목재
1008 화학
1009 의약품
1010 비금속광물
1011 철강금속
1012 기계
1013 전기전자
1014 의료정밀
1015 운수장비
1016 유통업
1017 전기가스업
1018 건설업
1019 운수창고업
1020 통신업
1021 금융업
1022 은행
1024 증권
1025 보험
1026 서비스업
1027 제조업
1002 코스피 대형주
1003 코스피 중형주
1004 코스피 소형주
1224 코스피 200 비중상한 30%
1227 코스피 200 비중상한 25%
1232 코스피 200 비중상한 20%
'''

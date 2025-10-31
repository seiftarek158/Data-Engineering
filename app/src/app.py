import pandas as pd
from scipy.stats import mstats
import numpy as np
from db_utils import save_to_db

def extract_data(data_path):
  return pd.read_csv(data_path)

def impute_missing_data(dtp, trades):
    dtp_copy = dtp.copy()
    trades_copy = trades.copy()
    # First we will convert the timestamp to date in order to compare it with the dtp
    trades_copy['date'] = pd.to_datetime(trades_copy['timestamp']).dt.date

    #Then we will calculate the estimated prices per date per stock from the trades dataframe
    estimated_prices_per_date_per_stock = trades_copy.groupby(['date', 'stock_ticker']).apply(
        lambda x: (x['cumulative_portfolio_value'] / x['quantity'] * x['quantity']).sum() / x['quantity'].sum()
    ).reset_index(name='price')
    len(estimated_prices_per_date_per_stock)

    for col in dtp_copy.columns:
        if col == 'date':
            continue
        
        # Get trade prices for this stock
        stock_prices = estimated_prices_per_date_per_stock[estimated_prices_per_date_per_stock['stock_ticker'] == col].set_index('date')['price']
        
        # Fill missing values from trades by checking the null rows and then mapping it to the correct stockprice using the date
        missing_mask = dtp_copy[col].isnull()
        dtp_copy.loc[missing_mask, col] = dtp_copy.loc[missing_mask, 'date'].map(stock_prices)
        
        # Fill any remaining with forward fill
        dtp_copy[col] = dtp_copy[col].fillna(method='ffill')
    dtp_copy['date'] = pd.to_datetime(dtp_copy['date'])
    rows = []
    for _, row in dtp_copy.iterrows():
        date = row['date']
        for ticker in dtp_copy.columns:
            if ticker != 'date':
                rows.append({
                    'date': date,
                    'stock_ticker': ticker,
                    'stock_price': row[ticker]
                })


    return pd.DataFrame(rows),trades_copy

def outlier_checker_IQR(column)->bool:
    series = column
    n = len(series)


    # Calculate IQR-based outliers
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers_mask = (series < lower) | (series > upper)
    n_outliers = outliers_mask.sum()
    pct_outliers = (n_outliers / n) * 100
    
    return pct_outliers<10  

def outlier_checker_Zscore(column)->bool:
    series = column
    n = len(series)

    # Calculate Z-score
    mean = series.mean()
    std = series.std()
    z_scores = (series - mean) / std if std else 0
    n_outliers = (abs(z_scores) > 3).sum()
    pct_outliers = (n_outliers / n) * 100

    return pct_outliers<10

def handle_outliers_logtransformation(df,col):
    df_copy = df.copy()
    if(df[col].min() <= 0):
        df_copy[col+"_log"] = np.log1p(df_copy[col])
    else:
        df_copy[col+"_log"] = np.log(df_copy[col])
    return df_copy

def handle_outliers_winsorizecapped(df,col):
    df_copy = df.copy()
    df_copy[col+"_winsorized"] = mstats.winsorize(df_copy[col], limits=[0.15, 0.15])
    return df_copy

def integrate_data(dtp, trades, dc, dd, ds):
   
    trades_joined_with_dc= trades.merge(dc, how='left', left_on='customer_id', right_on='customer_id')
    trades_and_dc_joined_with_ds= trades_joined_with_dc.merge(ds, how='left', left_on='stock_ticker', right_on='stock_ticker')
    dd['date'] = pd.to_datetime(dd['date']).dt.date
    trades_and_dc_and_ds_joined_with_dd= trades_and_dc_joined_with_ds.merge(dd[['date', 'day_name', 'is_weekend', 'is_holiday']], how='left', left_on='date', right_on='date')
    integrated=trades_and_dc_and_ds_joined_with_dd.copy()
    integrated = integrated.rename(columns={
    'account_type': 'customer_account_type',
    'liquidity_tier': 'stock_liquidity_tier',
    'sector': 'stock_sector',
    'industry': 'stock_industry'
        })
    # Merge integrated with dtp to get stock_price
    dtp['date'] = pd.to_datetime(dtp['date']).dt.date
    integrated = integrated.merge(dtp, 
                                on=['date', 'stock_ticker'], 
                                how='left')

    # Drop the temporary date column
    integrated.drop('date', axis=1, inplace=True)
    # Calculate total_trade_amount
    integrated['total_trade_amount'] = integrated['stock_price'] * integrated['quantity']
    required_columns = ['transaction_id', 'timestamp', 'customer_id', 'stock_ticker', 
                   'transaction_type', 'quantity', 'average_trade_size_winsorized', 'stock_price_log',
                   'total_trade_amount', 'customer_account_type', 'day_name', 
                   'is_weekend', 'is_holiday', 'stock_liquidity_tier', 
                   'stock_sector', 'stock_industry']
    integrated = integrated[required_columns]
    integrated.columns = [col.lower() for col in integrated.columns]
    #stock_price_log change to stock_price
    integrated = integrated.rename(columns={'stock_price_log': 'stock_price',
                                            'average_trade_size_winsorized': 'average_trade_size'})
    return integrated

if __name__ == '__main__':
    dtp = extract_data('data/daily_trade_prices.csv')
    trades = extract_data('data/trades.csv')
    dc = extract_data('data/dim_customer.csv')
    dd = extract_data('data/dim_date.csv')
    ds = extract_data('data/dim_stock.csv')


    dtp_imputed, trades = impute_missing_data(dtp, trades)

    cols = ['STK001','STK002','STK003','STK004','STK005','STK006','STK007','STK008','STK009','STK010',
        'STK011','STK012','STK013','STK014','STK015','STK016','STK017','STK018','STK019','STK020']
    num_of_outlier_columns = 0
    for col in cols:
        series = dtp_imputed.loc[dtp_imputed['stock_ticker'] == col, 'stock_price']
        if not outlier_checker_IQR(series):
            num_of_outlier_columns += 1
    if(num_of_outlier_columns > 0):
        dtp_imputed = handle_outliers_logtransformation(dtp_imputed, 'stock_price')
  
    if not outlier_checker_IQR(trades['cumulative_portfolio_value']):
        trades = handle_outliers_logtransformation(trades, 'cumulative_portfolio_value')

    if not outlier_checker_IQR(trades['quantity']):
        trades = handle_outliers_logtransformation(trades, 'quantity')

    if not outlier_checker_IQR(trades['average_trade_size']):
        trades = handle_outliers_winsorizecapped(trades, 'average_trade_size')

    if not outlier_checker_Zscore(dc['avg_trade_size_baseline']):
        dc = handle_outliers_winsorizecapped(dc, 'avg_trade_size_baseline')
    
    # Add date column to trades for integration
    
    
    integrated = integrate_data(dtp_imputed, trades, dc, dd, ds)


    save_to_db(integrated, 'integrated_trades_data')
    sample=pd.DataFrame(integrated.sample(10))
    sample.to_csv('data/sample_data.csv', index=False)

    
import pandas as pd
import numpy as np

def engineer_features(pdf: pd.DataFrame) -> pd.DataFrame:
    """Applies cyclical time encoding and rolling metrics to the raw Gold dataframe."""
    pdf = pdf.copy()
    
    # Time features
    pdf['hour_of_day'] = pdf['hour_start'].dt.hour
    pdf['day_of_week'] = pdf['hour_start'].dt.dayofweek
    pdf['is_weekend'] = pdf['day_of_week'].isin([5, 6]).astype(int)
    pdf['hour_sin'] = np.sin(2 * np.pi * pdf['hour_of_day'] / 24)
    pdf['hour_cos'] = np.cos(2 * np.pi * pdf['hour_of_day'] / 24)

    # Lags and Rolling Windows
    pdf['lag_1h'] = pdf['total_orders'].shift(1)
    pdf['lag_2h'] = pdf['total_orders'].shift(2)
    pdf['lag_3h'] = pdf['total_orders'].shift(3)
    pdf['rolling_mean_3h'] = pdf['total_orders'].shift(1).rolling(window=3).mean()
    pdf['rolling_mean_6h'] = pdf['total_orders'].shift(1).rolling(window=6).mean()
    pdf['rolling_std_6h'] = pdf['total_orders'].shift(1).rolling(window=6).std()

    return pdf.dropna()

def get_train_test_split(df_clean: pd.DataFrame):
    """Splits the clean dataframe into 80/20 sequential train and test sets."""
    split_index = int(len(df_clean) * 0.8)
    
    features = [
        'hour_sin', 'hour_cos', 'day_of_week', 'is_weekend', 
        'rolling_mean_3h', 'rolling_mean_6h', 'rolling_std_6h', 
        'lag_1h', 'lag_2h', 'lag_3h', 'avg_discount'
    ]
    target = 'total_orders'

    X_train = df_clean.iloc[:split_index][features]
    y_train = df_clean.iloc[:split_index][target]
    X_test = df_clean.iloc[split_index:][features]
    y_test = df_clean.iloc[split_index:][target]
    
    return X_train, y_train, X_test, y_test
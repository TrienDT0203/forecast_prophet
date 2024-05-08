class Forecast_Model():
    
    def __init__(self, historical_data, date_column, forecast_value):
        self.historical_data = historical_data.copy()
        self.date = date_column
        self.forecast_value = forecast_value

    def call_forecast_model(self, no_of_days_forecast):
        from prophet import  Prophet
        import pandas as pd 
        # init forecast model
        self.historical_data[self.date] = self.historical_data[self.date].astype('datetime64[ns]')
        self.historical_data.rename(columns = {
            self.date : 'ds',
            self.forecast_value: 'y'
        }, inplace = True)

        forecast_model = Prophet(
                yearly_seasonality= True,
                weekly_seasonality= True,
                interval_width = 0.8,       # <<<<<--------NEED TO CHECK AGAIN---------->>>>>
            )

        forecast_model.add_country_holidays(country_name= "Vietnam")

        forecast_model.add_seasonality(
                name='yearly',
                period=365,
                fourier_order=100,  
                prior_scale=50,    
            )

        forecast_model.add_seasonality(
                name='weekly',
                period=7,
                fourier_order=100,  
                prior_scale=50,    
            ) 
                            
                                # <<<<<<<<<<------ Should be split this func into create_model() and forecast() function at this line
        import pandas as pd

        # start to forecast
        forecast_model.fit(self.historical_data)

        future_dates = forecast_model.make_future_dataframe(periods= no_of_days_forecast, freq= "D")
        
        df_forecast = forecast_model.predict(future_dates)
        print(df_forecast.yhat.max())
        df_forecast = df_forecast[['ds','trend', 'yhat', 'yhat_lower',	'yhat_upper']]
        df_forecast.rename(columns = {
                            'ds': 'date',
                            'yhat': 'forecast_value',
                            'yhat_lower': 'forecast_lower',
                            'yhat_upper': 'forecast_upper'
                        }, inplace = True)
        

        # combine historical data and forecast data
        df_forecast = pd.merge(df_forecast,
                            self.historical_data,
                            left_on = 'date',
                            right_on = 'ds',
                            how= 'left')
        
        df_forecast = df_forecast[['date','forecast_value', 'forecast_lower', 'forecast_upper','trend','y']]
        
        return df_forecast
    
    def resample_and_plot_data(self, _df_forecast, start_date):
        df_forecast = _df_forecast.copy()
        self.historical_data['month'] = self.historical_data['ds'].dt.strftime('%Y-%m')
        self.historical_data = self.historical_data[ self.historical_data['ds'] >= start_date].groupby(['month'], as_index= False)['y'].sum()

        df_forecast['month'] = df_forecast['date'].apply( lambda x: x.strftime('%Y-%m'))
        df_forecast = df_forecast[df_forecast['date'] >= start_date ].groupby(['month'],as_index = False)[['forecast_value','forecast_lower','forecast_upper','trend']].sum()

        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter

        plt.figure(figsize=(20, 6))
        plt.plot(self.historical_data['month'], self.historical_data['y'], label='Historical Data', marker='o', linestyle='-', color='blue')
        plt.plot(df_forecast['month'], df_forecast['forecast_value'], label='Predicted Data', marker='o', linestyle='--', color='red')
        
        plt.fill_between(df_forecast['month'], df_forecast['forecast_lower'], df_forecast['forecast_upper'], color='k', alpha=0.2, label='Uncertainty Interval')
        plt.xlabel('Date')
        plt.ylabel('Forecasted Value')
        plt.legend()

        plt.grid(True)
        plt.xticks(df_forecast['month'], rotation=45, ha='right', rotation_mode='anchor')
        plt.subplots_adjust(left=0.1)  

                
        if df_forecast['forecast_value'].max() >= 1e9:
            plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, pos: '%1.1fB' % (x * 1e-9)))  # Apply custom formatter for y-axis ticks
            plt.ylabel('(Billion VND)')
        else:
            plt.gca().yaxis.set_major_formatter(FuncFormatter(lambda x, pos: '%1.1fK' % (x * 1e-3)))
            plt.ylabel('Thousand Stoppoints')  

        for i, txt in enumerate(df_forecast['forecast_value']):
            # Format the numbers as billions or thousands
            if txt >= 10**9:
                label = '{:.1f}B'.format(txt / 10**9)
            elif txt >= 10**3:
                label = '{:.1f}K'.format(txt / 10**3)
            else:
                label = str(txt)
            plt.text(df_forecast['month'].iloc[i], df_forecast['forecast_value'].iloc[i], label, ha='right')


        plt.show()

        plt.tight_layout()
        plt.show()

        return df_forecast

    def export_data(self,df,file_type, file_name):
        import os

        if os.path.exists('./data/data_forecast/{}.{}'.format(file_name, file_type)):
            print('File name exist. Try with other name!!!')
            return None
        
        try:
            if file_type == 'excel':
                df.to_excel('./data/data_forecast/{}.xlsx'.format(file_name))
                print('Export data sucessfully!')
            elif file_type == 'csv':
                df.to_csv('./data/data_forecast/{}.csv'.format(file_name))
                print('Export data sucessfully!')
        except:
            print('Export data uncessfully. Try again with another name or contact file owener!')

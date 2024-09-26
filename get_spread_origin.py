import os
import json
import requests
import pandas as pd
import numpy as np
import time

# host = r'http://10.8.100.83:40100/'
host = r'http://122.112.197.7:8093/stage-api-algo/'


def mypath(file_name, dir1, dir2='', dir3='', dir4='', dir5=''):
    current_dir = os.getcwd()
    project_root = os.path.dirname(current_dir)
    output_dir = os.path.join(project_root, dir1, dir2, dir3, dir4, dir5)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_path = os.path.join(output_dir, file_name)
    return file_path


class GetSpread:
    def __init__(self, workday):
        self.workday = workday
        self.year = workday.split('-')[0]

    def load_json(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return None
        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")
            return None

    def save_json(self, data, file_path):
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)

    def load_bond_data(self, file_path):
        try:
            data = pd.read_excel(file_path)
            # # 删除包含缺失值的行
            # data.dropna(subset=['StartDate', 'Maturity'], inplace=True)
            return data
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return None
        except Exception as e:
            print(f"Error loading bond data from file: {file_path}. Error: {str(e)}")
            return None

    def construct_bonds_payload(self, bonds_data, prepayment_data):
        bonds = []
        for index, row in bonds_data.iterrows():
            if pd.isna(row['StartDate']) or pd.isna(row['Maturity']):
                print(f"跳过缺少起息日期或到期日期的债券: {row}")
                continue
            bond_info = {
                "ID": row['BondCode'],
                "EffectiveDate": row['StartDate'].strftime('%Y-%m-%d'),
                "Maturity": row['Maturity'].strftime('%Y-%m-%d'),
                "PaymentFrequency": int(row['CouponFrequency']) if not pd.isna(row['CouponFrequency']) else 0 if row[
                                                                                                                     'CouponType'] == '到期一次还本付息' else -1,
                "MarketPrice": row['Close_DirtyPrice'],
                "CouponSchedule": {
                    row['StartDate'].strftime('%Y-%m-%d'): 0 if row['CouponType'] == '贴现' else row['Coupon'] / 100.0}
            }

            # 增加PrepaymentSchedule
            prepayment_schedule = prepayment_data[prepayment_data['thscode'] == row['BondCode']]
            prepay_dict = {}
            for _, prepay_row in prepayment_schedule.iterrows():
                # print(prepay_row['ths_pre_repay_principal_date_bond'])
                prepay_date = prepay_row['ths_pre_repay_principal_date_bond']
                # print(prepay_date)
                # print(pd.to_datetime(prepay_date, format='%Y%m%d').strftime('%Y-%m-%d'))
                prepay_ratio = prepay_row['ths_pre_repay_principal_ratio_bond']
                if not pd.isna(prepay_date) and not pd.isna(prepay_ratio):
                    prepay_dict[pd.to_datetime(prepay_date, format='%Y%m%d').strftime('%Y-%m-%d')] = prepay_ratio
                    # print(prepay_dict)
            if prepay_dict:
                bond_info["PrepaymentSchedule"] = prepay_dict

            # # try
            # error_file_name = f'error-json.json'
            # error_data = self.load_json(mypath(error_file_name, 'scripts'))
            # dirty_price_dict = {item['Id']: item['DirtyPrice'] for item in error_data['Data']['Results'] if 'DirtyPrice' in item}
            # dirty_price = dirty_price_dict.get(row['BondCode'], None)
            # bond_info['MarketPrice'] = dirty_price

            bonds.append(bond_info)

        # with open(f'error{self.workday}.json', 'w', encoding='utf-8') as file:
        #     json.dump(bonds, file, indent=4)

        # with open(f'error-124290.SH-{self.workday}.json', 'w', encoding='utf-8') as file:
        #         json.dump(bonds, file, indent=4)

        # print(f"传递给 API 的债券数据: {json.dumps(bonds, ensure_ascii=False)}")
        return bonds

    def construct_api_payload(self, bonds, fitted_curve, value_date):
        ##

        with open(f'error{self.workday}.json', 'w', encoding='utf-8') as file:
            json.dump(bonds, file, indent=4)

        payload = {
            "FittedCurve": fitted_curve,
            "ValueDate": value_date,
            "Bonds": bonds,
            "IncludeValueDateCashFlow": False
        }

        with open(f'error{self.workday}.json', 'w', encoding='utf-8') as file:
            json.dump(payload, file, indent=4)

        return payload

    def post_request(self, url, payload):
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"API request error: {e}")
            return None

    def run_valuation(self):
        start_time = time.time()

        bond_file_name = f'Urban-data-{self.workday}.xlsx'
        prepayment_file_name = 'prepayment.xlsx'
        fitted_curve_file_name = f'ret-[1.0, 1.0]-MonotoneConvex-{self.workday}-1.json'
        output_file_name = f'z-spread-data-{self.workday}.json'

        bond_file_path = mypath(bond_file_name, 'inputs', 'trade_data_urban', self.year)
        prepayment_path = mypath(prepayment_file_name, 'inputs', 'trade_data_urban')
        fitted_curve_file_path = mypath(fitted_curve_file_name, 'outputs', 'trade_data', 'ret', self.year)
        output_file_path = mypath(output_file_name, 'outputs', 'spread_data_new', self.year)
        # # try
        # output_file_path = mypath(output_file_name,'try', self.year)

        # # Skip if output file already exists
        # if os.path.exists(output_file_path):
        #     print(f"Output file already exists for {self.workday}. Skipping...")
        #     return

        bonds_data = self.load_bond_data(bond_file_path)
        prepayment_data = self.load_bond_data(prepayment_path)
        fitted_curve_data = self.load_json(fitted_curve_file_path)

        if bonds_data is None or fitted_curve_data is None:
            print(f"Skipping valuation for {self.workday} due to missing data.")
            return

        value_date = self.workday
        bonds = self.construct_bonds_payload(bonds_data, prepayment_data)
        fitted_curve = fitted_curve_data['Data']['curveParam']

        api_payload = self.construct_api_payload(bonds, fitted_curve, value_date)
        api_url = host + 'api/Bonds/ValuationAlgo2'

        api_response = self.post_request(api_url, api_payload)

        # # 输出 api_response 用于调试
        # print(f"API 响应: {api_response}")

        if api_response is None:
            print(f"API request failed for {self.workday}. Skipping...")
            return

        # 检查 'Data' 和 'Results' 是否存在
        if api_response.get('Data') is None or 'Results' not in api_response['Data']:
            print(f"Unexpected API response format for {self.workday}. Skipping...")
            return

        # if 'Data' not in api_response or 'Results' not in api_response['Data']:
        #     print(f"Unexpected API response format for {self.workday}. Skipping...")
        #     return

        results = api_response['Data']['Results']
        self.save_json(results, output_file_path)

        end_time = time.time()
        processing_time_minutes = (end_time - start_time) / 60
        print(f"Processed {self.workday} in {processing_time_minutes:.2f} minutes.")


def run_valuation_for_date_range(start_date, end_date):
    business_days = pd.bdate_range(start=start_date, end=end_date)
    total_start_time = time.time()

    for workday in business_days:
        workday_str = workday.strftime('%Y-%m-%d')
        spread_getter = GetSpread(workday_str)
        spread_getter.run_valuation()

    total_end_time = time.time()
    total_processing_time_minutes = (total_end_time - total_start_time) / 60
    print(
        f"Processed {len(business_days)} days from {start_date} to {end_date} in {total_processing_time_minutes:.2f} minutes.")


###############################################################
class GetPrice:
    def __init__(self, workday):
        self.workday = workday
        self.year = workday.split('-')[0]

    def load_json(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            return data
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return None
        except json.JSONDecodeError:
            print(f"Error decoding JSON from file: {file_path}")
            return None

    def save_json(self, data, file_path):
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4)

    def load_bond_data(self, file_path):
        try:
            data = pd.read_excel(file_path)
            # # 删除包含缺失值的行
            # data.dropna(subset=['StartDate', 'Maturity'], inplace=True)
            return data
        except FileNotFoundError:
            print(f"File not found: {file_path}")
            return None
        except Exception as e:
            print(f"Error loading bond data from file: {file_path}. Error: {str(e)}")
            return None

    def construct_bonds_payload(self, bonds_data, prepayment_data):
        bonds = []
        for index, row in bonds_data.iterrows():
            if pd.isna(row['StartDate']) or pd.isna(row['Maturity']):
                print(f"跳过缺少起息日期或到期日期的债券: {row}")
                continue
            bond_info = {
                "ID": row['BondCode'],
                "EffectiveDate": row['StartDate'].strftime('%Y-%m-%d'),
                "Maturity": row['Maturity'].strftime('%Y-%m-%d'),
                "PaymentFrequency": int(row['CouponFrequency']) if not pd.isna(row['CouponFrequency']) else 0 if row['CouponType'] == '到期一次还本付息' else -1,
                "CouponSchedule": {
                    row['StartDate'].strftime('%Y-%m-%d'): 0 if row['CouponType'] == '贴现' else row['Coupon'] / 100.0}
            }

            # 增加PrepaymentSchedule
            prepayment_schedule = prepayment_data[prepayment_data['thscode'] == row['BondCode']]
            prepay_dict = {}
            for _, prepay_row in prepayment_schedule.iterrows():
                # print(prepay_row['ths_pre_repay_principal_date_bond'])
                prepay_date = prepay_row['ths_pre_repay_principal_date_bond']
                prepay_ratio = prepay_row['ths_pre_repay_principal_ratio_bond']
                if not pd.isna(prepay_date) and not pd.isna(prepay_ratio):
                    prepay_dict[pd.to_datetime(prepay_date, format='%Y%m%d').strftime('%Y-%m-%d')] = prepay_ratio
                    # print(prepay_dict)
            if prepay_dict:
                bond_info["PrepaymentSchedule"] = prepay_dict

            bonds.append(bond_info)

        # with open(f'error{self.workday}.json', 'w', encoding='utf-8') as file:
        #     json.dump(bonds, file, indent=4)

        # with open(f'error-json-{self.workday}.json', 'w', encoding='utf-8') as file:
        #         json.dump(bonds, file, indent=4)

        # print(f"传递给 API 的债券数据: {json.dumps(bonds, ensure_ascii=False)}")
        return bonds

    def construct_spread_payload(self, oas_data):
        spread_param = []
        # Extract OAS data for the specific day
        if self.workday in oas_data.index.get_level_values('Date'):
            oas_for_day = oas_data.loc[self.workday]
            for bond_code, record in oas_for_day.iterrows():
                spread_info = {
                    'Bonds': bond_code,
                    "interpolationMethod": "Linear",
                    "fitParam1": [1],
                    "fitParam2": [float(record["full_prediction"])]
                }
                spread_param.append(spread_info)
        else:
            print(f'skip {self.workday} for missing date in OAS data')

        return spread_param

    def construct_api_payload(self, bonds, fitted_curve, value_date, spread_param):
        payloads = []
        # if spread_param.empty:
        spread_param_dict = {param['Bonds']: param for param in spread_param}
        # json.dump(spread_param_dict, open(mypath(f'error-spread_param_dict-{self.workday}.json', 'try', self.year), 'w', encoding='utf-8'), indent=4)
        # json.dump(bonds_dict, open(mypath(f'error-bonds_dict-{self.workday}.json', 'try', self.year), 'w', encoding='utf-8'), indent=4)

        for bond in bonds:
            
            bond_code = bond['ID']
            if bond_code in spread_param_dict:
                payload = {
                    "FittedCurve": fitted_curve,
                    "SpreadParam": spread_param_dict[bond_code],
                    "ValueDate": value_date,
                    "Bonds": [bond],
                    "IncludeValueDateCashFlow": False
                }
                payloads.append(payload)
        print(f'api payload: {len(bonds)}')
        return payloads

    def post_request(self, url, payload):
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"API request error: {e}")
            return None

    def run_valuation(self):
        start_time = time.time()

        bond_file_name = f'Urban-data-{self.workday}.xlsx'
        prepayment_file_name = 'prepayment.xlsx'
        fitted_curve_file_name = f'ret-[1.0, 1.0]-MonotoneConvex-{self.workday}-1.json'
        oas_file_name = 'OAS.pkl'
        output_file_name = f'price-data-{self.workday}.json'
        
        bond_file_path = mypath(bond_file_name, 'inputs', 'trade_data_urban_full', self.year)
        prepayment_path = mypath(prepayment_file_name, 'inputs', 'trade_data_urban')
        fitted_curve_file_path = mypath(fitted_curve_file_name, 'outputs', 'trade_data', 'ret', self.year)
        oas_file_path = mypath(oas_file_name, 'outputs', 'OAS')
        output_file_path = mypath(output_file_name, 'outputs', 'price_data', self.year)

        # try
        # output_file_path = mypath(output_file_name,'try', self.year)

        # Skip if output file already exists
        if os.path.exists(output_file_path):
            print(f"Output file already exists for {self.workday}. Skipping...")
            return

        bonds_data = self.load_bond_data(bond_file_path)
        prepayment_data = self.load_bond_data(prepayment_path)
        fitted_curve_data = self.load_json(fitted_curve_file_path)
        oas_data = pd.read_pickle(oas_file_path)

        if bonds_data is None or fitted_curve_data is None:
            print(f"Skipping valuation for {self.workday} due to missing data.")
            return

        if oas_data is None:
            print(f"Skipping valuation for {self.workday} due to oas missing data.")
            return

        value_date = self.workday
        bonds = self.construct_bonds_payload(bonds_data, prepayment_data)
        fitted_curve = fitted_curve_data['Data']['curveParam']
        spread_param = self.construct_spread_payload(oas_data)

        # api_payload = self.
        # (bonds, fitted_curve, value_date, spread_param)
        api_payloads = self.construct_api_payload(bonds, fitted_curve, value_date, spread_param)
        print(f'after construct api payload: {len(api_payloads)}')
        api_url = host + 'api/Bonds/ValuationAlgo2'
        results = []
        for i in range(len(api_payloads)):

            api_response = self.post_request(api_url, api_payloads[i])

            if api_response is None:
                print(f"API request failed for {self.workday}. Skipping...")
                return

            # 检查 'Data' 和 'Results' 是否存在
            if api_response.get('Data') is None or 'Results' not in api_response['Data']:
                print(f"Unexpected API response format for {self.workday}. Skipping...")
                return

            # if 'Data' not in api_response or 'Results' not in api_response['Data']:
            #     print(f"Unexpected API response format for {self.workday}. Skipping...")
            #     return

            result = api_response['Data']['Results']
            results.append(result)

        self.save_json(results, output_file_path)

        end_time = time.time()
        processing_time_minutes = (end_time - start_time) / 60
        print(f"Processed {self.workday} in {processing_time_minutes:.2f} minutes.")

    def generate_dataframe(self):
        bond_file_name = f'Urban-data-{self.workday}.xlsx'
        output_file_name = f'price-data-{self.workday}.json'

        bond_file_path = mypath(bond_file_name, 'inputs', 'trade_data_urban', self.year)
        output_file_path = mypath(output_file_name, 'outputs', 'price_data', self.year)
        # try
        # output_file_path = mypath(output_file_name,'try', self.year)

        # Load data
        bonds_data = self.load_bond_data(bond_file_path)
        api_response = self.load_json(output_file_path)

        # If bonds_data is None, create DataFrame using BondCodes from api_response with NaN Close_DirtyPrice
        if bonds_data is None and api_response is not None:
            bond_codes_from_api = [bond_result[0].get("Id") for bond_result in api_response if
                                   bond_result[0].get("Id") is not None]
            bonds_data = pd.DataFrame({
                "BondCode": bond_codes_from_api,
                "Close_DirtyPrice": [np.nan] * len(bond_codes_from_api)
            })

        if bonds_data is None and api_response is None:
            print(f"Skipping due to missing data for {self.workday}.")
            return None

        # 如果 bonds_data 仍然是 None，那么就跳过这一天的处理
        if bonds_data is None:
            print(f"Skipping due to missing bonds data for {self.workday}.")
            return None

        if api_response is None:
            print(f"Skipping due to missing api response for {self.workday}.")
            return None

        # Create a dictionary from bonds_data for quick lookup
        bonds_dict = bonds_data.set_index('BondCode').to_dict('index')
        

        # Create DataFrame with necessary columns
        result_data = []
        i = 0
        for bond_result in api_response:
            bond_code = bond_result[0].get("Id")
            dirty_price = bond_result[0].get("DirtyPrice")

            close_dirty_price = bonds_dict.get(bond_code, {}).get("Close_DirtyPrice", np.nan)

            result_data.append({
                "workday": self.workday,
                "BondCode": bond_code,
                "Close_DirtyPrice": close_dirty_price,
                "API_DirtyPrice": dirty_price
            })

            print(f'Close_DirtyPrice: {close_dirty_price}, API_DirtyPrice: {dirty_price}')

            i += 1
        print(f'bonds in price data: {i}')
        # Convert to DataFrame
        df = pd.DataFrame(result_data)
        return df


def run_price_for_date_range(start_date, end_date):
    business_days = pd.bdate_range(start=start_date, end=end_date)
    total_start_time = time.time()

    all_results = []

    for workday in business_days:
        workday_str = workday.strftime('%Y-%m-%d')
        price_getter = GetPrice(workday_str)
        price_getter.run_valuation()

        df = price_getter.generate_dataframe()
        if df is not None:
            all_results.append(df)

    # Combine all dataframes into one
    final_df = pd.concat(all_results, ignore_index=True)

    total_end_time = time.time()
    total_processing_time_minutes = (total_end_time - total_start_time) / 60
    print(
        f"Processed {len(business_days)} days from {start_date} to {end_date} in {total_processing_time_minutes:.2f} minutes.")
    print(final_df)
    return final_df



import requests
import datetime
import calendar
import json


# color scheme
class Col:
    YL = '\033[93m'
    BL = '\033[94m'
    OK = '\033[96m'


# ---------------------- EDIT THE VARIABLES BELOW TO ENSURE YOU GET THE RIGHT OUTPUT ----------------------
# Report Service API docs for reference: https://help.adjust.com/en/article/report-service-api
year = 2023  # provide the year for which you want to pull the data
month = 2  # provide the month for which you want to pull the data
path = '/Users/username/data/2023-02'  # provide correct path to store the output files
app_token = 'xxxxxxxxxxxxxxx'  # provide the app_token for which you want to pull the data
attribution_source = 'dynamic'  # dynamic OR first
attribution_kind = 'all'  # all OR click OR impression
sandbox = 'false'  # false OR true
cost_mode = 'network'  # network OR adjust
api_token = 'xxxxxxxxxxxxxxxxxxxxxxxxxxx'  # provide your api token from Adjust dashboard to authorize requests

dimensions = [  # add or remove dimensions from the list below to modify the output
    'partner_name', 'network', 'campaign_network', 'campaign_id_network', 'adgroup', 'adgroup_id_network',
    'creative', 'creative_network', 'source_network', 'source_id_network', 'country', 'country_code', 'os_name', 'hour'
]

metrics = [  # add or remove metrics from the list below to modify the output
    'installs', 'clicks', 'impressions', 'network_installs', 'network_cost', 'network_ecpi', 'cost'
]
# ---------------------------------------------------------------------------------------------------------

# request params
csv_endpoint = "https://dash.adjust.com/control-center/reports-service/csv_report?"
json_endpoint = "https://dash.adjust.com/control-center/reports-service/report?"
header = {"Authorization": "Bearer " + api_token}

# defining date ranges
dates = [datetime.date(year, month, day).strftime('%Y-%m-%d') for day in range(1, calendar.monthrange(year, month)[1]+1)]
dates_total = len(dates) - 1
month_start_date = dates[0]
month_end_date = dates[-1]

# getting the list of country codes for request breakdown
cc_request_url = ('%sapp_token=%s&date_period=%s:%s&dimensions=country_code&metrics=%s&sandbox=%s'
                  % (json_endpoint, app_token, month_start_date, month_end_date, ','.join(metrics), sandbox))

print("Fetching available country codes...")
cc_response = json.loads(requests.get(cc_request_url, headers=header).text)["rows"]

country_codes = []

for row in cc_response:
    country_codes.append(row["country_code"])

cc_total = len(country_codes) - 1

print("Country codes fetching completed. Found " + Col.BL + str(cc_total) + Col.OK + " country codes.")
print("Data fetch started. The estimated number of requests to process: " + Col.BL + str(cc_total * dates_total) + Col.OK)

# fetching the data from the API day by day, country by country
log_file_path = ('%s/%s_%s__%s.txt' % (path, app_token, month_start_date, month_end_date))
print("Log file located here: " + Col.BL + log_file_path + Col.OK)

for day in dates:
    processed_count = count_200 = count_201 = count_error = 0

    for cc in country_codes:
        # progress output
        print("\rProcessed requests for %s: %s / %s. Success: %s, errors: %s" % (
            day, Col.BL + str(processed_count) + Col.OK, Col.BL + str(cc_total) + Col.OK,
            Col.BL + str(count_200 + count_201) + Col.OK,
            Col.YL + str(count_error) + Col.OK), end='')

        # fetching data from API
        date_period = day + ':' + day
        file_name = ('%s/%s_%s_%s.csv' % (path, app_token, cc, day))
        request_url = ('%sapp_token=%s&date_period=%s&dimensions=%s&metrics=%s&country_code__in=%s&cost_mode=%s&attribution_kind=%s&attribution_source=%s&sandbox=%s'
                       %
                       (csv_endpoint, app_token, date_period, ','.join(dimensions), ','.join(metrics), cc, cost_mode, attribution_kind, attribution_source, sandbox))

        response = requests.get(request_url, headers=header)

        # writing response
        with open(file_name, 'a+') as current_file:
            current_file.write(response.text)

        # ------------------------------------- PROGRESS OUTPUT + LOGGING -------------------------------------
        status = response.status_code
        if status == 200:
            count_200 += 1
        elif status == 201:
            count_201 += 1
        else:
            count_error += 1

        processed_count += 1

        # writing log
        log_line = date_period + ' ' + cc + ' processed. ' + str(response.status_code) + ' ' + str(response)
        with open(log_file_path, 'a+') as log_file:
            log_file.write(log_line + "\n")
        # -------------------------------------------------------------------------------------------------------

    print("Completed processing for %s. Total requests: %s, success - %s, no data - %s, errors - %s. Log: %s - " % (day, cc_total, count_200, count_201, count_error, log_file_path))
print(Col.BL + 'Process completed.')

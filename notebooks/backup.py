### Commercial

BUILDING_CHARACTERISTICS_FILE = 'DC_upgrade0_agg.xlsx'
BUILDING_CHARACTERISTICS_FILEPATH = DATA_DIR + 'background/' + BUILDING_CHARACTERISTICS_FILE
commercial_building_characteristics = pd.read_excel(BUILDING_CHARACTERISTICS_FILEPATH, sheet_name='building_characteristics')

def construct_neighborhood_commercial():


    warehouse_list = []
    small_office_list = []
    medium_office_list = []
    hospital_list = []
    outpatient_list = []
    primary_school_list = []
    secondary_school_list = []

    small_office_list.extend(commercial_building_characteristics[commercial_building_characteristics['in.comstock_building_type'] == 'SmallOffice']['bldg_id'].tolist())
    warehouse_list.extend(commercial_building_characteristics[commercial_building_characteristics['in.comstock_building_type'] == 'Warehouse']['bldg_id'].tolist())
    medium_office_list.extend(commercial_building_characteristics[commercial_building_characteristics['in.comstock_building_type'] == 'MediumOffice']['bldg_id'].tolist())
    hospital_list.extend(commercial_building_characteristics[commercial_building_characteristics['in.comstock_building_type'] == 'Hospital']['bldg_id'].tolist())
    outpatient_list.extend(commercial_building_characteristics[commercial_building_characteristics['in.comstock_building_type'] == 'Outpatient']['bldg_id'].tolist())
    primary_school_list.extend(commercial_building_characteristics[commercial_building_characteristics['in.comstock_building_type'] == 'PrimarySchool']['bldg_id'].tolist())
    secondary_school_list.extend(commercial_building_characteristics[commercial_building_characteristics['in.comstock_building_type'] == 'SecondarySchool']['bldg_id'].tolist())
   
    total_commercial_building_list = small_office_list + warehouse_list + medium_office_list + hospital_list + outpatient_list + primary_school_list + secondary_school_list
    
    total_commercial_building_chosen = random.choices(total_commercial_building_list, k=2)
   

#    commercial_building_list = commercial_building_characteristics['bldg_id'].tolist()

 #   commercial_building_chosen = random.choices(commercial_building_list, k=math.ceil(TOTAL_BUILDINGS * 0.10)) # assuming commercial buildings are 10% of total buildings
    

    return [total_commercial_building_chosen]

#df_weather = pd.read_csv('./data/weather/USA_DC_Washington.National.AP.725030_TMY3.csv')
df_weather = pd.read_csv('./data/G1100010_2018.csv')
df_weather['date_time'] = pd.to_datetime(df_weather['date_time'], format='%m/%d/%y %H:%M')
df_weather.set_index(df_weather['date_time'])
df_weather_daily = df_weather.resample('d', on='date_time').mean()
df_weather_daily.head()
df_weather_daily.to_csv(f'{DIR}amy2018_average_monthly.csv')



total_merged_buildings_hourly = pd.concat(total_merged_buildings_hourly_list)
total_merged_buildings_hourly.to_csv(f'total_merged_community_load_profile_{timestamp_str}.csv')
total_merged_buildings_hourly_average = total_merged_buildings_hourly.groupby(total_merged_buildings_hourly.index).mean()
ax_total_merged_buildings_hourly_average = total_merged_buildings_hourly_average.plot(x='timestamp', y='out.electricity.total.energy_consumption..kwh', figsize=(15,5))

fig2 = ax_total_merged_buildings_hourly_average.get_figure()
fig2.savefig(f'community_load_profile_{timestamp_str}.png')


test_building = pd.read_parquet('./data/timeseries_individual_buildings/upgrade=0/state=DC/549845-0.parquet')

test_building.columns[test_building.columns.str.contains('total')].to_list()


new_test_building = test_building[['timestamp','out.electricity.total.energy_consumption..kwh']]
print("type: ", type(new_test_building['timestamp']))
new_test_building['out.electricity.total.energy_consumption..kwh'] = new_test_building['out.electricity.total.energy_consumption..kwh'] * 100.0
new_test_building.plot(x='timestamp', y='out.electricity.total.energy_consumption..kwh', figsize=(15,5))





new_test_building.index = pd.to_datetime(new_test_building['timestamp'])
print(pd.infer_freq(new_test_building.index))
new_test_building_hourly = new_test_building.resample('h')['out.electricity.total.energy_consumption..kwh'].sum()

new_test_building_hourly.plot(x='timestamp', y='out.electricity.total.energy_consumption..kwh', figsize=(15,5))

new_test_building_hourly.head()

new_test_building_hourly_sorted = new_test_building_hourly.sort_values(ascending=False)
new_test_building_hourly_sorted = new_test_building_hourly_sorted.reset_index()
new_test_building_hourly_sorted['Ascending'] = np.arange(1, len(new_test_building_hourly_sorted) + 1) / 8760.0
new_test_building_hourly_sorted

new_test_building_hourly_sorted.plot(x='Ascending', y='out.electricity.total.energy_consumption..kwh', figsize=(15,5))
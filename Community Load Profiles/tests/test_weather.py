import tempfile
from pathlib import Path
from microgrid_profiles.weather import read_weather_csv_auto

def test_weather_auto():
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)/"w.csv"
        p.write_text("DATE,TEMP\n2020-01-01 00:00,10\n2020-01-01 01:00,12\n")
        ws = read_weather_csv_auto(p, preferred_units="C")
        assert "outdoor_air_temperature" in ws.df.columns

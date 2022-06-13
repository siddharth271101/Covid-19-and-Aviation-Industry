import pandas as pd
import numpy as np

def adjust_date(s):
    l = s.split("/")
    return f"20{l[2]}-{int(l[0]):02d}-{int(l[1]):02d}"


base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
confirmed_url = "time_series_covid19_confirmed_global.csv"
dead_url = "time_series_covid19_deaths_global.csv"
recovered_url = "time_series_covid19_recovered_global.csv"

confirmed = pd.read_csv(base_url + confirmed_url)
confirmed["Province/State"] = confirmed["Province/State"].fillna("")
dead = pd.read_csv(base_url + dead_url)
dead["Province/State"] = dead["Province/State"].fillna("")
recovered = pd.read_csv(base_url + recovered_url)
recovered["Province/State"] = recovered["Province/State"].fillna("")

# these are the country-province combinations, we go through them to
# reformat the columns
combos = np.array(
    confirmed[["Country/Region", "Province/State"]].to_records(index=False)
)

data = pd.DataFrame()
for country, state in combos:

    # Here we just need to check if we are dealing with a country-province
    # or just a country
    is_na = False
    if state == "":
        is_na = True

    df_c = confirmed[
        (confirmed["Country/Region"] == country)
        & ((confirmed["Province/State"] == state))
    ]

    try:
        Lat, Long = df_c["Lat"].values[0], df_c["Long"].values[0]
    except IndexError:
        print("  Skipping, Index Error")
        continue
    except Exception as e:
        raise (e)
    # print(Lat, Long)

    df_r = recovered.loc[
        (recovered["Country/Region"] == country)
        & ((recovered["Province/State"] == state))
    ].transpose()
    df_r = df_r.drop(["Province/State", "Country/Region", "Lat", "Long"])

    # Canada is annoying and doesn't report recovered at a province level
    try:
        df_r.columns = ["Recovered"]
    except:
        df_r["Recovered"] = np.NaN  # May change this to the empty string

    df_c = df_c.transpose().drop(
        ["Province/State", "Country/Region", "Lat", "Long"]
    )
    df_c.columns = ["Confirmed"]

    df_d = dead[
        (dead["Country/Region"] == country)
        & ((dead["Province/State"] == state))
    ].transpose()
    df_d = df_d.drop(["Province/State", "Country/Region", "Lat", "Long"])
    df_d.columns = ["Deaths"]

    df = df_c
    df["Recovered"] = df_r["Recovered"]
    df["Deaths"] = df_d["Deaths"]
    df["Country/Region"] = country
    df["Province/State"] = state
    df["Date"] = df.index

    data = pd.concat([data,
    df[
        [
            "Date",
            "Country/Region",
            "Province/State",
            "Confirmed",
            "Recovered",
            "Deaths",
        ]
    ]
                 ]
)

data["Date"] = data.index
data["Date"] = data["Date"].map(adjust_date)
data = data.reset_index(drop=True)
data.to_csv("dags/data/covid.csv", index=False)
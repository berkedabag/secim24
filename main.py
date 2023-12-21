import psycopg2
import folium
import requests
import time
import random
import os
from folium.plugins import MarkerCluster
import streamlit as st
import pandas as pd
import psycopg2


#source venv/bin/activate
#streamlit run main.py
# Open a cursor to perform database operations
conn = psycopg2.connect(
    dbname="secim24",
    user="postgres",
    password="",
    host="localhost"
)

m = folium.Map(location=[39.92667430466246, 32.84679082652187], zoom_start=11)
marker_cluster = MarkerCluster().add_to(m)

cur = conn.cursor()

# Execute a SELECT statement on secim_result19 table to fetch Muhtarlık Id and Sandık No 4342  + 745
cur.execute("SELECT DISTINCT \"İlçe Adı\", \"Muhtarlık Adı\", \"latitude\", \"longitude\" FROM secim_results19_v2") # assuming you have latitude and longitude in your table
locations = cur.fetchall()


def fetch_data(ilce=None, mahalle=None):
    # Prepare SQL query using LIKE operator for filtering
    cur.execute("""
    SELECT DISTINCT "İlçe Adı", "Muhtarlık Adı"
    FROM secim_results19_v2
    WHERE "İlçe Adı" LIKE %s AND "Muhtarlık Adı" LIKE %s
    """, ('%' + ilce + '%', '%' + mahalle + '%'))
    
    # Fetch filtered locations based on user input
    filtered_locations = cur.fetchall()
    print(filtered_locations)
    results = []
    for ilce, mahalle in filtered_locations:
        print(f"İlçe: {ilce}, Mahalle: {mahalle}")
        
        # Using parameterized query instead of f-string
        cur.execute("""
        SELECT SUM("saadet" + BTP + "tkp" + "VATAN PARTİSİ" + CHP + "AK PARTİ" + dsp + "SABİT TEKİN" + "MEHMET CERİT" + "MERİÇ MEYDAN" + "MEHMET HOŞOĞLU" + "RECEP GÖKYER") 
        FROM secim_results19_v2 
        WHERE "İlçe Adı" = %s AND "Muhtarlık Adı" = %s
        """, (ilce, mahalle))
        
        tum_oylar_2019 = cur.fetchone()[0]
        print(f"tum_oylar_2019: {tum_oylar_2019}")
        
        cur.execute(f""" SELECT SUM("chp") 
                    FROM secim_results19_v2 
                    WHERE "İlçe Adı" = '{ilce}' AND "Muhtarlık Adı" = '{mahalle}'
                    """)
        chp_total_2019 = cur.fetchone()[0]
        if chp_total_2019 == 0:
            continue
        print(f"chp_total_2019: {chp_total_2019}")
        chp_totale_orani =  chp_total_2019 / tum_oylar_2019* 100
        print(f"chp_totale_orani: {chp_totale_orani}")

        # Sum votes for 2023
        cur.execute(f"""
        SELECT SUM("RECEP TAYYİP ERDOĞAN" + "KEMAL KILIÇDAROĞLU")
        FROM secim_results23_cb 
        WHERE "İlçe Adı" = '{ilce}' AND "Muhtarlık Adı" = '{mahalle}'
        """)
        tum_oylar_2023 = cur.fetchone()[0]
        if tum_oylar_2023 == None:
            tum_oylar_2023 = 0
        print(f"tum_oylar_2023: {tum_oylar_2023}")
        
        cur.execute(f""" SELECT SUM(\"KEMAL KILIÇDAROĞLU\") 
                    FROM secim_results23_cb
                    WHERE "İlçe Adı" = '{ilce}' AND "Muhtarlık Adı" = '{mahalle}'
                    """)
        muhalefet_total_2023 = cur.fetchone()[0]
        if muhalefet_total_2023 == None:
            muhalefet_total_2023 = 0

        if muhalefet_total_2023 == 0:
            continue
        print(f"muhalefet_total_2023(CHP, İYİP, ZP): {muhalefet_total_2023}")

        muhalefet_totale_orani = muhalefet_total_2023  / tum_oylar_2023 * 100
        print(f"muhalefet_totale_orani: {muhalefet_totale_orani}")

        total_oy_degisimi = tum_oylar_2023 - tum_oylar_2019
        print(f"total_oy_degisimi: {total_oy_degisimi}")
    
        muhalefet_oy_degisimi = muhalefet_total_2023 - chp_total_2019 
        print(f"muhalefet_oy_degisimi: {muhalefet_oy_degisimi}")

        muhalefet_oy_orani_degisimi = muhalefet_totale_orani - chp_totale_orani

        print(f"muhalefet_oy_orani_degisimi: {muhalefet_oy_orani_degisimi}")

        
    
            #... Your logic to fetch and calculate values ...

        results.append({
            "İlçe": ilce,
            "Mahalle": mahalle,
            "2019 Toplam Geçerli Oy Sayısı": tum_oylar_2019,
            "2019 CHP Oyları": chp_total_2019,
            "2019 CHP %": "%{:.2f}".format(chp_totale_orani),
            "2023 Toplam Geçerli Oy Sayısı": tum_oylar_2023,
            "2023 KK": muhalefet_total_2023,
            "2023 KK %": "%{:.2f}".format(muhalefet_totale_orani),
            "Toplam Geçerli oy Sayısı Değişimi": total_oy_degisimi,
            "KK Oy Değişimi": muhalefet_oy_degisimi,
            "KK % Değişimi": "%{:.2f}".format(muhalefet_oy_orani_degisimi)
        })

    return pd.DataFrame(results)


st.set_page_config(page_title="Seçim Karşılaştırma", page_icon=None, layout="centered", initial_sidebar_state="auto", menu_items=None)

        
cur.execute("SELECT DISTINCT \"İlçe Adı\" FROM secim_results19_v2")
all_ilce = [row[0] for row in cur.fetchall()]
all_ilce = sorted(all_ilce)

# Fetch all distinct neighborhoods (mahalle) for a given district (ilçe)
def fetch_mahalle_for_ilce(ilce):
    cur.execute("SELECT DISTINCT \"Muhtarlık Adı\" FROM secim_results19_v2 WHERE \"İlçe Adı\" = %s", (ilce,))
    all_mahalle = [row[0] for row in cur.fetchall()]
    return sorted(all_mahalle)

# Streamlit UI
st.title("Seçim Karşılaştırma - 2019 Belediye vs 2023 CB")

# Use the fetched ilce values to populate the dropdown
selected_ilce = st.selectbox("Lütfen ilçe seçiniz:", all_ilce)

# Fetch 'Mahalle' based on selected 'İlçe' and populate the dropdown
available_mahalle = fetch_mahalle_for_ilce(selected_ilce)
selected_mahalle = st.selectbox("Lütfen Mahalleyi seçiniz:", available_mahalle)

if st.button('Sonuçları Getir'):
    if selected_ilce and selected_mahalle:
        data = fetch_data(selected_ilce, selected_mahalle.upper())
        st.table(data)
    else:
        st.warning("Lütfen bir mahalle seçiniz")

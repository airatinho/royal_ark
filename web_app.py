import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import json
config = json.load(open("files/config.json", encoding='utf-8'))


st.title('Загрузка файлов и отображение результатов')
st.header("Загрузка csv файлов в БД")
st.write("_________________________")
uploaded_files = st.file_uploader(
    "Загрузить csv файлы",help ='Загрузка файлов',
    accept_multiple_files=True,type={"csv"}
)
events=None
installs=None
def show_uploaded_files(upl_fls:st.file_uploader)->None:
    """Показывает загруженные csv файлы"""
    global events,installs
    if upl_fls is not None:
        for uploaded_file in upl_fls:
             st.write("filename:", uploaded_file.name)
             if "events" in uploaded_file.name:
                 events=pd.read_csv(uploaded_file).drop(columns=['Unnamed: 0'],errors='ignore')
                 st.dataframe(events)
             elif "install" in uploaded_file.name:
                 installs=pd.read_csv(uploaded_file).drop(columns=['Unnamed: 0'],errors='ignore')
                 st.dataframe(installs)
             else:
                 st.write("Нужно загрузить events и installs файлы")
show_uploaded_files(uploaded_files)

def download_to_db(ev:pd.DataFrame=None,inst:pd.DataFrame=None)->None:
    """Загружает данные в таблицу"""
    engine = create_engine(f"{config['db']}://{config['login_db']}:{config['password_db']}"
                           f"@{config['domain']}:{config['port']}/{config['db_name']}", echo=False)

    try:
        if ev is not None: #сделал так, чтобы потом была возможность грузить по одной
            ev.to_sql('events', con=engine, if_exists='replace')
        if inst is not None:
            inst.to_sql('installs', con=engine, if_exists='replace')
        st.write("Таблицы загружены!")
    except Exception:
        st.write("Ошибка загрузки данных в бд!")
if (installs is not None) and (events is not None):
    if st.button("Загрузить таблицы в бд Postgres"):
        download_to_db(ev=events,inst=installs)

st.header("Суммарное количество заработанного и количества установок")
st.write("_________________________")
company=st.text_input("Название компании", "Введите название компании")
start_date=st.date_input("Дата с ",pd.to_datetime('2019-01-30'))
end_date=st.date_input("Дата по ",pd.to_datetime('2020-01-30'))
@st.cache
def convert_df(df):
   return df.to_csv().encode('utf-8')
if st.button("Посчитать"):
    if (installs is not None) and (events is not None):
        if company !="Введите название компании":
            res_ev=events[
                (events['install_time'].apply(pd.to_datetime) > pd.to_datetime(start_date)) &
                (events['install_time'].apply(pd.to_datetime) < pd.to_datetime(end_date)) &
                (events['campaign'] == company)].groupby('campaign').agg({'event_revenue': "sum"}).sum()[0]
            st.write(f"Сумма заработка от всех компаний : {res_ev}")

            res_inst = installs[
                (installs['install_time'].apply(pd.to_datetime) > pd.to_datetime(start_date)) &
                (installs['install_time'].apply(pd.to_datetime) < pd.to_datetime(end_date)) &
                (installs['campaign'] == company)].groupby('campaign').agg({'install_time': "count"}).sum()[0]
            st.write(f"Количество скачиваний от всех компаний{res_inst}")

        else:
            st.write("Сумма заработка от всех компаний")
            res_ev = events[
                (events['install_time'].apply(pd.to_datetime) > pd.to_datetime(start_date)) &
                (events['install_time'].apply(pd.to_datetime) < pd.to_datetime(end_date))
                ].groupby('campaign').agg({'event_revenue': "sum"})
            st.dataframe(res_ev)
            csv_ev=convert_df(res_ev)
            st.download_button(
                               "Скачать",
                               csv_ev,
                               "res_ev.csv",
                               "text/csv",
                               key='download-csv'
                                            )
            st.write("Количество скачиваний от всех компаний")
            res_inst = installs[
                (installs['install_time'].apply(pd.to_datetime) > pd.to_datetime(start_date)) &
                (installs['install_time'].apply(pd.to_datetime) < pd.to_datetime(end_date))
                ].groupby('campaign').agg({'install_time': "count"})
            st.dataframe(res_inst)
            csv_inst = convert_df(res_inst)
            st.download_button(
                                "Скачать",
                                csv_inst,
                                "res_inst.csv",
                                "text/csv",
                                key='download-csv'
            )
    else:
        st.write("Файлы не загружены")
# st.download_button('Скачать отчет CSV', res_inst.to_csv("Installs sum.csv",index=False,), 'csv')
# st.download_button('Скачать отчет CSV', res_ev.to_csv("Events sum.csv",index=False), 'csv')
# evets=[if 'events' in uploaded_files]
# st.dataframe(installs)
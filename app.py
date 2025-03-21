
import streamlit as st
import pyarrow.dataset as ds
import pandas as pd
import matplotlib.pyplot as plt
import altair as alt

# 페이지 제목
st.title("2024 KOBIS Daily Box Office 데이터 분석")

@st.cache_data 

def load_data():
    path = "/home/jacob/data/movie_after/dailyboxoffice"
    dataset = ds.dataset(path, format="parquet", partitioning="hive")
    df = dataset.to_table().to_pandas()

    
    df['dt'] = pd.to_datetime(df['dt'], format='%Y%m%d', errors='coerce')

    
    def pseudo_null_score(row):
        return sum([
            row["multiMovieYn"] == "Unclassified",
            row["repNationCd"] == "Unclassified"
        ])

   
    df["null_score"] = df.apply(pseudo_null_score, axis=1)

    group_cols = ["dt", "movieCd", "movieNm"]

 
    deduped_df = df.sort_values(by="null_score").groupby(group_cols, observed=True).head(1)

    deduped_df = deduped_df.drop(columns=["null_score"])
    final_df = deduped_df.drop(columns=['rank', 'rnum', 'rankInten', 'salesShare', 'rankOldAndNew'])

    return final_df

df = load_data()
unique_dates = df["dt"].dropna().dt.date.unique()
selected_date = st.selectbox("날짜 선택", sorted(unique_dates, reverse=True))

# 선택한 날짜 데이터만 보기
filtered_df = df[df["dt"].dt.date == selected_date]
st.write(f"📅 {selected_date} 기준 박스오피스:")
st.dataframe(filtered_df.reset_index(drop=True))


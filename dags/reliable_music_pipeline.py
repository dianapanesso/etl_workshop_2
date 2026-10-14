from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd
import sqlalchemy
import great_expectations as gx

@dag(
    dag_id="reliable_music_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
)
def music_data_pipeline():

    @task
    def extract_spotify() -> str:
        df = pd.read_csv('/opt/airflow/data/spotify_dataset.csv')

        # INYECCIÓN DE ERROR PARA EL TEST B
        # Forzamos que la primera canción tenga una popularidad de 999 (el límite es 100)
        #df.loc[0, 'popularity'] = 999



        output_path = '/opt/airflow/data/spotify_raw_temp.csv'
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def validate_spotify_raw(file_path: str) -> str:
        df = pd.read_csv(file_path)
        ge_df = gx.from_pandas(df)
        res_track = ge_df.expect_column_values_to_not_be_null('track_id')
        res_pop = ge_df.expect_column_values_to_be_between('popularity', min_value=0, max_value=100)
        
        if not (res_track["success"] and res_pop["success"]):
            raise ValueError("¡Validación fallida en Spotify crudo!")
        return file_path

    @task
    def extract_grammys() -> str:
        df = pd.read_csv('/opt/airflow/data/grammys_clean.csv')
        output_path = '/opt/airflow/data/grammys_raw_temp.csv'
        df.to_csv(output_path, index=False)
        return output_path

    @task
    def validate_grammys_raw(file_path: str) -> str:
        df = pd.read_csv(file_path)
        ge_df = gx.from_pandas(df)
        res_year = ge_df.expect_column_values_to_not_be_null('year')
        
        if not res_year["success"]:
            raise ValueError("¡Validación fallida en Grammys crudo!")
        return file_path

    @task
    def transform_and_integrate(spotify_path: str, grammys_path: str) -> str:
        df_spotify = pd.read_csv(spotify_path)
        df_grammys = pd.read_csv(grammys_path)
        
        # Normalizamos nombres para un cruce más flexible si es necesario, 
        # o aseguramos un outer/inner join correcto según la estructura del taller.
        df_spotify['artists_lower'] = df_spotify['artists'].astype(str).str.lower().str.strip()
        
        # Verificamos qué columna de nombres o artistas tienen los grammys
        col_grammy_artist = 'workers' if 'workers' in df_grammys.columns else ('artist' if 'artist' in df_grammys.columns else None)
        
        if col_grammy_artist:
            df_grammys['artist_lower'] = df_grammys[col_grammy_artist].astype(str).str.lower().str.strip()
            # Usamos un inner o left join robusto. Si quieres conservar todo el catálogo de los Grammys:
            df_integrated = pd.merge(
                df_grammys, 
                df_spotify, 
                left_on='artist_lower', 
                right_on='artists_lower', 
                how='left', 
                suffixes=('_grammy', '_spotify')
            )
        else:
            df_integrated = df_grammys.copy()
            
        output_path = '/opt/airflow/data/integrated_prepared.csv'
        df_integrated.to_csv(output_path, index=False)
        return output_path

    @task
    def validate_prepared(file_path: str) -> str:
        df = pd.read_csv(file_path)
        ge_df = gx.from_pandas(df)
        res_rows = ge_df.expect_table_row_count_to_be_between(min_value=1, max_value=None)
        
        if not res_rows["success"]:
            raise ValueError("¡Validación fallida en los datos preparados!")
        return file_path

    @task
    def load_dw(file_path: str):
        engine = sqlalchemy.create_engine('mysql+mysqlconnector://uao_user:uao_password@grammy_mysql:3306/grammy_db')
        df = pd.read_csv(file_path)
        
        # 1. Cargar la dimensión de años completa desde el archivo original de Grammys
        df_grammys_orig = pd.read_csv('/opt/airflow/data/grammys_clean.csv')
        dim_year = df_grammys_orig[['year']].drop_duplicates().dropna().sort_values(by='year').reset_index(drop=True)
        dim_year['year'] = pd.to_numeric(dim_year['year'], errors='coerce').astype(int)
        dim_year = dim_year.drop_duplicates().sort_values(by='year').reset_index(drop=True)
        dim_year['year_id'] = dim_year.index + 1
        dim_year.to_sql('dim_year', engine, if_exists='replace', index=False)
        
        # 2. Dimensión Artista basada en los datos integrados reales
        col_artist_final = 'artists' if 'artists' in df.columns else ('workers' if 'workers' in df.columns else 'artist')
        df[col_artist_final] = df[col_artist_final].fillna('Unknown Artist').astype(str)
        
        dim_artist = df[[col_artist_final]].drop_duplicates().rename(columns={col_artist_final: 'artists'}).reset_index(drop=True)
        dim_artist['artist_id'] = dim_artist.index + 1
        dim_artist.to_sql('dim_artist', engine, if_exists='replace', index=False)
        
        # 3. Dimensión Categoría extraída directamente de los Grammys para tener todas las categorías reales
        col_cat_orig = 'category' if 'category' in df_grammys_orig.columns else 'category_grammy'
        dim_category = df_grammys_orig[[col_cat_orig]].dropna().drop_duplicates().rename(columns={col_cat_orig: 'category'}).reset_index(drop=True)
        dim_category['category_id'] = dim_category.index + 1
        dim_category.to_sql('dim_category', engine, if_exists='replace', index=False)
        
        # 4. Tabla de Hechos
        fact_df = df.copy()
        
        # Normalizar año y categoría para los cruces de llaves foráneas
        fact_df['year_val'] = pd.to_numeric(fact_df['year'] if 'year' in fact_df.columns else 1997, errors='coerce').fillna(1997).astype(int)
        
        col_cat_fact = 'category' if 'category' in fact_df.columns else ('category_grammy' if 'category_grammy' in fact_df.columns else None)
        fact_df['cat_val'] = fact_df[col_cat_fact].fillna('Sin Categoría').astype(str) if col_cat_fact else 'Sin Categoría'
        fact_df['art_val'] = fact_df[col_artist_final].astype(str)
        
        # Mapear IDs con las dimensiones
        fact_df = fact_df.merge(dim_artist, left_on='art_val', right_on='artists', how='left')
        fact_df = fact_df.merge(dim_year, left_on='year_val', right_on='year', how='left')
        fact_df = fact_df.merge(dim_category, left_on='cat_val', right_on='category', how='left')
        
        # Limpieza de columnas y selección final para la tabla de hechos
        cols_hechos = ['artist_id', 'year_id', 'category_id', 'track_id', 'track_name', 'album_name', 'popularity', 'winner']
        final_cols = [c for c in cols_hechos if c in fact_df.columns]
        fact_table = fact_df[final_cols].dropna(subset=['artist_id', 'year_id', 'category_id'])
        
        fact_table.to_sql('fact_music_performance', engine, if_exists='replace', index=False)
        print("Modelo dimensional cargado correctamente con todas las categorías y datos reales.")

        
    spotify_raw = extract_spotify()
    spotify_val = validate_spotify_raw(spotify_raw)

    grammys_raw = extract_grammys()
    grammys_val = validate_grammys_raw(grammys_raw)

    integrated_data = transform_and_integrate(spotify_val, grammys_val)
    validated_prepared = validate_prepared(integrated_data)
    load_dw(validated_prepared)

music_data_pipeline()
import os
import sys
import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import streamlit as st

# ================== CONFIG ==================

API_KEY_SIRENE = "baec8347-d5ad-4056-ac83-47d5ad10565e"

SIRENE_URL = "https://api.insee.fr/api-sirene/3.11/siret/{}"
BAN_SEARCH_URL = "https://api-adresse.data.gouv.fr/search/"

QPV_GEO_PATH = "QP2024_France_Hexagonale_Outre_Mer_WGS84.gpkg"
ZRR_CSV_PATH = "ZRR_list_source.csv"

COL_CODE_QP = "code_qp"
COL_LIB_QP = "lib_qp"
COL_LIB_COM = "lib_com"
ZRR_LIB_COL = "LIBGEO"

# ================== CHARGEMENT DONNÉES ==================

@st.cache_resource
def load_qpv_polygones(path: str) -> gpd.GeoDataFrame:
    if not os.path.exists(path):
        return gpd.GeoDataFrame()
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        raise ValueError("Le fichier QPV n'a pas de CRS.")
    if gdf.crs.to_epsg() != 2154:
        gdf = gdf.to_crs(epsg=2154)
    return gdf

@st.cache_resource
def load_zrr_data(path: str):
    if not os.path.exists(path):
        return pd.DataFrame(), set()
    df_zrr = pd.read_csv(path, header=5)
    df_zrr["CODGEO"] = df_zrr["CODGEO"].astype(str).str.zfill(5)
    communes_zrr = set(
        df_zrr.loc[
            df_zrr["ZRR_SIMP"].str.startswith(("C", "P"), na=False),
            "CODGEO",
        ].tolist()
    )
    return df_zrr, communes_zrr

# ================== FONCTIONS METIER ==================

def calcul_proximite_qpv(pt_wgs: Point, qpv_gdf: gpd.GeoDataFrame):
    if pt_wgs is None or qpv_gdf.empty:
        return None

    pt_proj = gpd.GeoSeries([pt_wgs], crs="EPSG:4326").to_crs(qpv_gdf.crs).iloc[0]

    mask_inside = qpv_gdf.contains(pt_proj)
    qpv_inside = qpv_gdf[mask_inside]
    est_dans_qpv = not qpv_inside.empty

    qpv_dans_lesquels = []
    if est_dans_qpv:
        for _, row in qpv_inside.iterrows():
            qpv_dans_lesquels.append({
                "code_qp": row.get(COL_CODE_QP),
                "lib_qp": row.get(COL_LIB_QP),
                "commune_qp": row.get(COL_LIB_COM),
            })

    distances_m = qpv_gdf.geometry.distance(pt_proj)
    min_dist_m = float(distances_m.min())
    distance_km = min_dist_m / 1000.0
    a_moins_1km_qpv = distance_km <= 1

    idx_min = distances_m.idxmin()
    row_min = qpv_gdf.loc[idx_min]
    qpv_plus_proche = {
        "code_qp": row_min.get(COL_CODE_QP),
        "lib_qp": row_min.get(COL_LIB_QP),
        "commune_qp": row_min.get(COL_LIB_COM),
        "distance_km": distance_km,
    }

    return {
        "est_dans_qpv": est_dans_qpv,
        "distance_km": distance_km,
        "a_moins_1km_qpv": a_moins_1km_qpv,
        "qpv_dans_lesquels": qpv_dans_lesquels,
        "qpv_plus_proche": qpv_plus_proche,
    }

def check_zrr_statut(code_commune: str, df_zrr: pd.DataFrame, communes_zrr: set):
    if not code_commune:
        return None, None
    code_commune = str(code_commune).zfill(5)
    is_zrr = code_commune in communes_zrr
    zrr_label = None
    if is_zrr:
        row_zrr = df_zrr.loc[df_zrr["CODGEO"] == code_commune]
        if not row_zrr.empty:
            zrr_label = row_zrr.iloc[0].get(ZRR_LIB_COL)
    return is_zrr, zrr_label

# --- SIRET ---

def get_sirene_etab(siret: str) -> dict:
    headers = {"X-INSEE-Api-Key-Integration": API_KEY_SIRENE}
    r = requests.get(SIRENE_URL.format(siret), headers=headers, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Erreur API SIRENE ({r.status_code})")
    data = r.json()
    return data.get("etablissement")

def analyse_depuis_siret(siret: str):
    qpv_gdf = load_qpv_polygones(QPV_GEO_PATH)
    df_zrr, communes_zrr = load_zrr_data(ZRR_CSV_PATH)
    etab = get_sirene_etab(siret)
    
    # Adresse Sirene
    adr = etab.get("adresseEtablissement", {})
    numero = adr.get("numeroVoieEtablissement") or ""
    type_voie = adr.get("typeVoieEtablissement") or ""
    lib_voie = adr.get("libelleVoieEtablissement") or ""
    code_postal = adr.get("codePostalEtablissement") or ""
    commune_nom = adr.get("libelleCommuneEtablissement") or ""
    
    adresse_full = f"{numero} {type_voie} {lib_voie}, {code_postal} {commune_nom}".strip()
    
    # Infos entreprise
    ul = etab.get("uniteLegale", {}) or {}
    nom_ent = ul.get("denominationUniteLegale") or etab.get("denominationUsuelleEtablissement")
    
    # Code commune
    code_commune = adr.get("codeCommuneEtablissement")
    in_zrr, zrr_label = check_zrr_statut(code_commune, df_zrr, communes_zrr)

    # Géocodage BAN
    pt_wgs = None
    r_ban = requests.get(BAN_SEARCH_URL, params={"q": adresse_full, "limit": 1})
    if r_ban.status_code == 200:
        d_ban = r_ban.json()
        if d_ban.get("features"):
            lon, lat = d_ban["features"][0]["geometry"]["coordinates"]
            pt_wgs = Point(lon, lat)

    res_qpv = calcul_proximite_qpv(pt_wgs, qpv_gdf)

    return {
        "type": "siret",
        "nom_entreprise": nom_ent,
        "adresse": adresse_full,
        "code_commune": code_commune,
        "in_zrr": in_zrr,
        "zrr_label": zrr_label,
        "qpv_data": res_qpv
    }

# --- ADRESSE DIRECTE ---

def analyse_depuis_adresse_raw(adresse_saisie: str):
    qpv_gdf = load_qpv_polygones(QPV_GEO_PATH)
    df_zrr, communes_zrr = load_zrr_data(ZRR_CSV_PATH)

    r = requests.get(BAN_SEARCH_URL, params={"q": adresse_saisie, "limit": 1}, timeout=10)
    if r.status_code != 200:
        raise RuntimeError("Erreur API Adresse")
    
    data = r.json()
    if not data.get("features"):
        raise ValueError("Adresse non trouvée.")

    feat = data["features"][0]
    props = feat.get("properties", {})
    geom = feat.get("geometry", {})
    
    label_adresse = props.get("label")
    code_commune = props.get("citycode") # Code INSEE
    lon, lat = geom.get("coordinates")
    pt_wgs = Point(lon, lat)

    in_zrr, zrr_label = check_zrr_statut(code_commune, df_zrr, communes_zrr)
    res_qpv = calcul_proximite_qpv(pt_wgs, qpv_gdf)

    return {
        "type": "adresse",
        "adresse_trouvee": label_adresse,
        "code_commune": code_commune,
        "in_zrr": in_zrr,
        "zrr_label": zrr_label,
        "qpv_data": res_qpv
    }


# ================== UI STREAMLIT ==================

st.set_page_config("ZRR & QPV Checker", layout="wide")
st.title("🔍 Vérification ZRR & QPV")

# --> TON LOGO ICI <--
st.logo("image.png", size="large", link=None, icon_image=None)

with st.sidebar:
    st.markdown("### ℹ️ À propos")
    st.write("Cet outil permet de vérifier l'éligibilité ZRR et QPV (1km) soit par SIRET, soit directement par Adresse.")
    st.write("Si le géocodage échoue pour le QPV ou que le ZRR est indéterminé, dans la grande majorité des cas c'est que l'adresse n'est ni dans un QPV, ni dans un ZRR. ")

# --- FORMULAIRES ---

col_input, col_res = st.columns([1, 1.5])

with col_input:
    st.markdown("### Option 1 : Par SIRET")
    siret_input = st.text_input("Numéro SIRET (14 chiffres)", placeholder="123 456 789 00011")
    btn_siret = st.button("Analyser ce SIRET")

    st.markdown("---") # Séparateur visuel

    st.markdown("### Option 2 : Par Adresse")
    adresse_input = st.text_input("Adresse complète", placeholder="10 rue de la Paix, 75000 Paris")
    btn_adresse = st.button("Analyser cette adresse")

# --- ANALYSE & AFFICHAGE ---

res = None
error_msg = None

if btn_siret and siret_input:
    with st.spinner("Analyse du SIRET..."):
        try:
            siret_clean = "".join(c for c in siret_input if c.isdigit())
            res = analyse_depuis_siret(siret_clean)
        except Exception as e:
            error_msg = str(e)

if btn_adresse and adresse_input:
    with st.spinner("Analyse de l'adresse..."):
        try:
            res = analyse_depuis_adresse_raw(adresse_input)
        except Exception as e:
            error_msg = str(e)

# --- RESULTATS ---

with col_res:
    if error_msg:
        st.error(f"Erreur : {error_msg}")

    if res:
        st.markdown("## 📊 Résultats")
        
        # Bloc Localisation
        with st.container(border=True):
            st.caption("Localisation identifiée")
            if res["type"] == "siret":
                st.write(f"🏢 **{res.get('nom_entreprise')}**")
                st.write(f"📍 {res.get('adresse')}")
            else:
                st.write(f"🏠 **Adresse normalisée :**")
                st.write(f"📍 {res.get('adresse_trouvee')}")
            st.write(f"🔢 Code INSEE : `{res.get('code_commune')}`")

        # Bloc ZRR
        in_zrr = res.get("in_zrr")
        zrr_nom = res.get("zrr_label")
        
        if in_zrr:
            st.success(f"✅ **ZRR : OUI** (Commune : {zrr_nom})")
        elif in_zrr is False:
            st.error("❌ **ZRR : NON**")
        else:
            st.warning("⚠️ **ZRR : Indéterminé**")

        # Bloc QPV
        qpv = res.get("qpv_data")
        if qpv:
            dist = qpv["distance_km"]
            is_close = qpv["a_moins_1km_qpv"]
            
            if is_close:
                st.success(f"✅ **Proximité QPV : OUI** (< 1km)")
            else:
                st.info(f"❌ **Proximité QPV : NON** (> 1km)")
            
            st.write(f"📏 Distance : **{dist:.3f} km**")
            
            if qpv["qpv_plus_proche"]:
                qp = qpv["qpv_plus_proche"]
                st.caption(f"QPV le plus proche : {qp['lib_qp']} ({qp['commune_qp']})")
            
            if qpv["est_dans_qpv"]:
                st.warning("🚨 L'adresse est située **DANS** le périmètre QPV.")
        else:
            st.warning("⚠️ Impossible de calculer la distance QPV (géocodage échoué).")
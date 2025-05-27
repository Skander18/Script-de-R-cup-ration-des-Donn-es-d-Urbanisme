

"""
Script de récupération des données d'urbanisme via WFS
Description : Ce script télécharge les données de zonage urbain depuis le Géoportail de l'urbanisme
"""

import requests
import geopandas as gpd
import pandas as pd
from io import BytesIO
import time

# Configuration
WFS_URL = "https://data.geopf.fr/wfs/ows"
LAYER_NAME = "wfs_du:zone_urba"
CRS = "EPSG:4326"
MAX_FEATURES = 5000 # Limite du serveur

# Emprise de la France entière en WGS84
BBOX_FRANCE = (-5.0, 41.0, 10.0, 52.0)

# Taille initiale des tuiles (en degrés)
INITIAL_TILE_SIZE = 1.0

# Fonction pour générer les tuiles
def generate_tiles(minx, miny, maxx, maxy, size):
    """Générateur de tuiles pour couvrir la zone spécifiée"""
    x = minx
    while x < maxx:
        y = miny
        while y < maxy:
            yield (x, y, min(x + size, maxx), min(y + size, maxy))
            y += size
        x += size

def get_features(bbox):
    """Récupère les features pour une BBOX donnée"""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": LAYER_NAME,
        "srsName": CRS,
        "outputFormat": "application/json",
        "bbox": ",".join(map(str, bbox)) + "," + CRS,
        "count": MAX_FEATURES
    }
    
    try:
        response = requests.get(WFS_URL, params=params, timeout=60)
        response.raise_for_status()
        return gpd.read_file(BytesIO(response.content))
    except Exception as e:
        print(f"Erreur pour BBOX {bbox}: {str(e)}")
        return None

def process_tile(bbox, depth=0):
    """Traite une tuile avec subdivision automatique si nécessaire pour récupérer toutes les entités"""
    MAX_DEPTH = 3  # Limite de subdivision
    TILE_DIVISION_FACTOR = 2  # Division par 2 à chaque niveau
    
    gdf = get_features(bbox)
    if gdf is None:
        return []
        
    if len(gdf) < MAX_FEATURES or depth >= MAX_DEPTH:
        cols_to_keep = ["gpu_doc_id", "partition", "nomfic", "geometry"] # Colonnes à conserver dans les résultats
        available_cols = [c for c in cols_to_keep if c in gdf.columns]
        return [gdf[available_cols]]
    
    # Subdivision nécessaire
    print(f"Subdivision de la tuile {bbox} (niveau {depth+1})")
    minx, miny, maxx, maxy = bbox
    new_size_x = (maxx - minx) / TILE_DIVISION_FACTOR
    new_size_y = (maxy - miny) / TILE_DIVISION_FACTOR
    
    results = []
    for i in range(TILE_DIVISION_FACTOR):
        for j in range(TILE_DIVISION_FACTOR):
            sub_bbox = (
                minx + i * new_size_x,
                miny + j * new_size_y,
                minx + (i+1) * new_size_x,
                miny + (j+1) * new_size_y
            )
            results.extend(process_tile(sub_bbox, depth+1))
    
    return results

# Collecte des données
all_features = []
total_collected = 0

# Nombre total de tuiles initiales
total_tiles = sum(1 for _ in generate_tiles(*BBOX_FRANCE, INITIAL_TILE_SIZE))
print(f"Nombre total de tuiles initiales à traiter: {total_tiles}\n")

for i, bbox in enumerate(generate_tiles(*BBOX_FRANCE, INITIAL_TILE_SIZE)):
    print(f"\nTraitement tuile initiale {i+1}/{total_tiles} - BBOX: {bbox}")
    
    tile_features = process_tile(bbox)
    if not tile_features:
        continue
        
    for gdf in tile_features:
        if len(gdf) > 0:
            all_features.append(gdf)
            total_collected += len(gdf)
            print(f"Collecté: {len(gdf)} entités (Total: {total_collected})")
    
    time.sleep(1)  

# Fusion et export
if all_features:
    final_gdf = gpd.GeoDataFrame(pd.concat(all_features, ignore_index=True))
    print(f"\nRésultat final: {len(final_gdf)} entités collectées")
    
    # Export GeoJSON
    final_gdf.to_file("urbanisme_complet.geojson", driver="GeoJSON")
    
    # Export CSV
    if "geometry" in final_gdf.columns:
        final_gdf.drop(columns="geometry").to_csv("urbanisme_attributs.csv", index=False)
    else:
        final_gdf.to_csv("urbanisme_attributs.csv", index=False)
    
    print("Export terminé - Fichiers créés:")
    print("- urbanisme_complet.geojson")
    print("- urbanisme_attributs.csv")
else:
    print("Aucune donnée n'a pu être collectée")

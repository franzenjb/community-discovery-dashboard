from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from datetime import datetime

gis = GIS("home")
print(f"Connected as: {gis.users.me.username}")

activities_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Community_Discovery_Visualization/FeatureServer/0")
chapter_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/3")
county_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/5")
summary_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Community_Discovery_Summary/FeatureServer/0")

print("[1/6] Fetching data...")
activities_sdf = activities_layer.query(where="1=1", out_fields="*", return_geometry=True, out_sr=4326).sdf
print(f"Activities: {len(activities_sdf)}")

# Find creator column
print(f"Columns: {list(activities_sdf.columns)}")
creator_col = None
for col in activities_sdf.columns:
    if 'creator' in col.lower():
        creator_col = col
        break

if creator_col:
    total_individuals = activities_sdf[creator_col].nunique()
    print(f"Unique individuals (from '{creator_col}'): {total_individuals}")
else:
    total_individuals = 0
    print("WARNING: No creator column found")

chapters_sdf = chapter_layer.query(where="1=1", out_fields="Chapter,Region,Division", return_geometry=True, out_sr=4326).sdf
print(f"Chapters: {len(chapters_sdf)}")

counties_sdf = county_layer.query(where="1=1", out_fields="County,State", return_geometry=True, out_sr=4326).sdf
print(f"Counties: {len(counties_sdf)}")

print("[2/6] Spatial join with Chapters...")
joined_chapters = activities_sdf.spatial.join(chapters_sdf, how="left", op="within")
print(f"Joined chapters: {len(joined_chapters)}")

cols_to_drop = [c for c in joined_chapters.columns if 'index_' in c.lower()]
if cols_to_drop:
    joined_chapters = joined_chapters.drop(columns=cols_to_drop)
    print(f"Dropped columns: {cols_to_drop}")

print("[3/6] Spatial join with Counties...")
joined_all = joined_chapters.spatial.join(counties_sdf, how="left", op="within")
print(f"Joined all: {len(joined_all)}")

print("\nSample results:")
print(joined_all[['Chapter', 'Region', 'Division', 'County', 'State']].head(10))

print("\n[4/6] Generating summaries...")
summaries = []
now_ms = int(datetime.utcnow().timestamp() * 1000)

# Total record
summaries.append({
    'attributes': {
        'geo_type': 'Total',
        'geo_name': 'All Activities',
        'parent_name': '',
        'activity_count': len(activities_sdf),
        'individual_count': total_individuals,
        'last_updated': now_ms
    }
})
print(f"Total: {len(activities_sdf)} activities, {total_individuals} individuals")

# Individual records - handle blank names as "Anonymous"
if creator_col:
    individual_counts = activities_sdf.groupby(creator_col).size().to_dict()
    for name, count in individual_counts.items():
        # Handle blank/empty/null names
        display_name = str(name).strip() if name else ""
        if not display_name or display_name == 'nan' or display_name == 'None':
            display_name = "Anonymous"
        summaries.append({'attributes': {
            'geo_type': 'Individual',
            'geo_name': display_name,
            'parent_name': '',
            'activity_count': int(count),
            'individual_count': 0,
            'last_updated': now_ms
        }})
    print(f"Individual records: {len(individual_counts)}")

# Division records
div_counts = joined_all.groupby('Division').size().to_dict()
for name, count in div_counts.items():
    if name and str(name) != 'nan':
        summaries.append({'attributes': {
            'geo_type': 'Division',
            'geo_name': str(name),
            'parent_name': '',
            'activity_count': int(count),
            'individual_count': 0,
            'last_updated': now_ms
        }})

# Region records
reg_data = joined_all.groupby(['Region', 'Division']).size().reset_index(name='count')
for _, row in reg_data.iterrows():
    if row['Region'] and str(row['Region']) != 'nan':
        summaries.append({'attributes': {
            'geo_type': 'Region',
            'geo_name': str(row['Region']),
            'parent_name': str(row['Division'] or ''),
            'activity_count': int(row['count']),
            'individual_count': 0,
            'last_updated': now_ms
        }})

# Chapter records
chap_data = joined_all.groupby(['Chapter', 'Region']).size().reset_index(name='count')
for _, row in chap_data.iterrows():
    if row['Chapter'] and str(row['Chapter']) != 'nan':
        summaries.append({'attributes': {
            'geo_type': 'Chapter',
            'geo_name': str(row['Chapter']),
            'parent_name': str(row['Region'] or ''),
            'activity_count': int(row['count']),
            'individual_count': 0,
            'last_updated': now_ms
        }})

# County records
county_data = joined_all.groupby(['County', 'State', 'Chapter']).size().reset_index(name='count')
for _, row in county_data.iterrows():
    if row['County'] and str(row['County']) != 'nan':
        summaries.append({'attributes': {
            'geo_type': 'County',
            'geo_name': str(row['County']),
            'parent_name': f"{row['State'] or ''} | {row['Chapter'] or ''}",
            'activity_count': int(row['count']),
            'individual_count': 0,
            'last_updated': now_ms
        }})

print(f"Generated {len(summaries)} summary records")

print("\n[5/6] Updating summary table...")
existing = summary_layer.query(where="1=1", return_geometry=False, out_fields="*")
if existing.features:
    oid_field = [k for k in existing.features[0].attributes.keys() if 'objectid' in k.lower()][0]
    oids = [f.attributes[oid_field] for f in existing.features]
    summary_layer.edit_features(deletes=oids)
    print(f"Deleted {len(oids)} old records")

print("[6/6] Adding new records...")
result = summary_layer.edit_features(adds=summaries)
print(f"✅ DONE! Added {sum(1 for r in result['addResults'] if r['success'])} records.")
print(f"   Total individuals: {total_individuals}")

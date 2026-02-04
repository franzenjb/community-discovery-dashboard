# Notebook Update: Add Individuals Metric

## Step 1: Add Field to Summary Table

In ArcGIS Online, add a new field to `Community_Discovery_Summary`:
- **Field Name:** `individual_count`
- **Type:** Integer
- **Allow Null:** Yes

## Step 2: Replace Cell 2 with this updated code

```python
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

# Count unique individuals (Creator field is the AGOL username of submitter)
if 'Creator' in activities_sdf.columns:
    total_individuals = activities_sdf['Creator'].nunique()
    print(f"Unique Individuals: {total_individuals}")
else:
    # Fallback: check for other possible field names
    creator_cols = [c for c in activities_sdf.columns if 'creator' in c.lower() or 'submitted' in c.lower() or 'user' in c.lower()]
    print(f"Available creator-like columns: {creator_cols}")
    if creator_cols:
        total_individuals = activities_sdf[creator_cols[0]].nunique()
        print(f"Unique Individuals (from {creator_cols[0]}): {total_individuals}")
    else:
        total_individuals = 0
        print("WARNING: No Creator field found. Individual count will be 0.")

chapters_sdf = chapter_layer.query(where="1=1", out_fields="Chapter,Region,Division", return_geometry=True, out_sr=4326).sdf
print(f"Chapters: {len(chapters_sdf)}")

counties_sdf = county_layer.query(where="1=1", out_fields="County,State", return_geometry=True, out_sr=4326).sdf
print(f"Counties: {len(counties_sdf)}")

print("[2/6] Spatial join with Chapters...")
joined_chapters = activities_sdf.spatial.join(chapters_sdf, how="left", op="within")
print(f"Joined chapters: {len(joined_chapters)}")

# Drop index columns before second join
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

# === NEW: Add a "Total" record with individual count ===
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
print(f"Added Total record: {len(activities_sdf)} activities, {total_individuals} individuals")

# Division summaries (with unique individuals per division)
div_counts = joined_all.groupby('Division').agg({
    'Division': 'size',
    'Creator': 'nunique' if 'Creator' in joined_all.columns else 'size'
}).rename(columns={'Division': 'activity_count', 'Creator': 'individual_count'})

for name, row in div_counts.iterrows():
    if name and str(name) != 'nan':
        summaries.append({
            'attributes': {
                'geo_type': 'Division',
                'geo_name': str(name),
                'parent_name': '',
                'activity_count': int(row['activity_count']),
                'individual_count': int(row.get('individual_count', 0)),
                'last_updated': now_ms
            }
        })

# Region summaries
if 'Creator' in joined_all.columns:
    reg_data = joined_all.groupby(['Region', 'Division']).agg({
        'Region': 'size',
        'Creator': 'nunique'
    }).reset_index(drop=True)
    reg_data.columns = ['count', 'individuals']
    reg_groups = joined_all.groupby(['Region', 'Division']).first().reset_index()[['Region', 'Division']]
    reg_data = reg_groups.join(reg_data)
else:
    reg_data = joined_all.groupby(['Region', 'Division']).size().reset_index(name='count')
    reg_data['individuals'] = 0

for _, row in reg_data.iterrows():
    if row['Region'] and str(row['Region']) != 'nan':
        summaries.append({
            'attributes': {
                'geo_type': 'Region',
                'geo_name': str(row['Region']),
                'parent_name': str(row['Division'] or ''),
                'activity_count': int(row['count']),
                'individual_count': int(row.get('individuals', 0)),
                'last_updated': now_ms
            }
        })

# Chapter summaries
chap_data = joined_all.groupby(['Chapter', 'Region']).size().reset_index(name='count')
for _, row in chap_data.iterrows():
    if row['Chapter'] and str(row['Chapter']) != 'nan':
        summaries.append({
            'attributes': {
                'geo_type': 'Chapter',
                'geo_name': str(row['Chapter']),
                'parent_name': str(row['Region'] or ''),
                'activity_count': int(row['count']),
                'individual_count': 0,  # Optional: add per-chapter individuals if needed
                'last_updated': now_ms
            }
        })

# County summaries
county_data = joined_all.groupby(['County', 'State', 'Chapter']).size().reset_index(name='count')
for _, row in county_data.iterrows():
    if row['County'] and str(row['County']) != 'nan':
        summaries.append({
            'attributes': {
                'geo_type': 'County',
                'geo_name': str(row['County']),
                'parent_name': f"{row['State'] or ''} | {row['Chapter'] or ''}",
                'activity_count': int(row['count']),
                'individual_count': 0,  # Optional: add per-county individuals if needed
                'last_updated': now_ms
            }
        })

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
success_count = sum(1 for r in result['addResults'] if r['success'])
print(f"✅ DONE! Added {success_count} records.")
print(f"   Total Activities: {len(activities_sdf)}")
print(f"   Unique Individuals: {total_individuals}")
```

## What Changed

1. **Added unique individual counting** using the `Creator` field (AGOL username)
2. **Added a "Total" record** (geo_type = 'Total') that stores:
   - Total activity count
   - Total unique individuals
3. **Added `individual_count` field** to each summary record

## Dashboard will read the "Total" record

The dashboard JavaScript will look for the record where `geo_type === 'Total'` to get the individual count.

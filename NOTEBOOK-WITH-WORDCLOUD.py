from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from datetime import datetime
import re
from collections import Counter

gis = GIS("home")
print(f"Connected as: {gis.users.me.username}")

activities_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Community_Discovery_Visualization/FeatureServer/0")
chapter_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/3")
county_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Master_ARC_Geography_2022/FeatureServer/5")
summary_layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Community_Discovery_Summary/FeatureServer/0")

print("[1/7] Fetching data...")
activities_sdf = activities_layer.query(where="1=1", out_fields="*", return_geometry=True, out_sr=4326).sdf
print(f"Activities: {len(activities_sdf)}")
print(f"Columns: {list(activities_sdf.columns)}")

# Find creator column (or use first_name/last_name if available)
creator_col = None
first_name_col = None
last_name_col = None

for col in activities_sdf.columns:
    col_lower = col.lower()
    if 'first' in col_lower and 'name' in col_lower:
        first_name_col = col
    if 'last' in col_lower and 'name' in col_lower:
        last_name_col = col
    if col_lower == 'creator':
        creator_col = col

# Determine how to identify individuals
if first_name_col and last_name_col:
    print(f"Using name fields: {first_name_col}, {last_name_col}")
    activities_sdf['_individual'] = activities_sdf[first_name_col].fillna('') + ' ' + activities_sdf[last_name_col].fillna('')
    activities_sdf['_individual'] = activities_sdf['_individual'].str.strip()
    activities_sdf.loc[activities_sdf['_individual'] == '', '_individual'] = 'Anonymous'
elif creator_col:
    print(f"Using Creator field: {creator_col}")
    activities_sdf['_individual'] = activities_sdf[creator_col].fillna('Anonymous')
    activities_sdf.loc[activities_sdf['_individual'].str.strip() == '', '_individual'] = 'Anonymous'
else:
    print("WARNING: No name or creator field found")
    activities_sdf['_individual'] = 'Unknown'

total_individuals = activities_sdf['_individual'].nunique()
print(f"Unique individuals: {total_individuals}")

# Find description column for word cloud
desc_col = None
for col in activities_sdf.columns:
    if 'describe' in col.lower() or 'briefly' in col.lower():
        desc_col = col
        break

print(f"Description column: {desc_col}")

chapters_sdf = chapter_layer.query(where="1=1", out_fields="Chapter,Region,Division", return_geometry=True, out_sr=4326).sdf
print(f"Chapters: {len(chapters_sdf)}")

counties_sdf = county_layer.query(where="1=1", out_fields="County,State", return_geometry=True, out_sr=4326).sdf
print(f"Counties: {len(counties_sdf)}")

print("[2/7] Spatial join with Chapters...")
joined_chapters = activities_sdf.spatial.join(chapters_sdf, how="left", op="within")
print(f"Joined chapters: {len(joined_chapters)}")

cols_to_drop = [c for c in joined_chapters.columns if 'index_' in c.lower()]
if cols_to_drop:
    joined_chapters = joined_chapters.drop(columns=cols_to_drop)

print("[3/7] Spatial join with Counties...")
joined_all = joined_chapters.spatial.join(counties_sdf, how="left", op="within")
print(f"Joined all: {len(joined_all)}")

print("[4/7] Extracting word cloud data...")
# Common words to exclude
stopwords = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
             'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
             'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
             'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need',
             'about', 'into', 'through', 'during', 'before', 'after', 'above',
             'below', 'between', 'under', 'again', 'further', 'then', 'once',
             'here', 'there', 'when', 'where', 'why', 'how', 'all', 'each',
             'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
             'only', 'own', 'same', 'so', 'than', 'too', 'very', 'just', 'also',
             'now', 'our', 'their', 'this', 'that', 'these', 'those', 'i', 'we',
             'you', 'he', 'she', 'it', 'they', 'what', 'which', 'who', 'whom',
             'my', 'your', 'his', 'her', 'its', 'us', 'them', 'am'}

word_counts = Counter()
if desc_col:
    for desc in activities_sdf[desc_col].dropna():
        # Extract words, remove punctuation, lowercase
        words = re.findall(r'\b[a-zA-Z]{3,}\b', str(desc))
        for word in words:
            w = word.lower()
            if w not in stopwords and len(w) >= 3:
                # Capitalize for display
                word_counts[word.capitalize()] += 1

print(f"Top 20 words: {word_counts.most_common(20)}")

print("\n[5/7] Generating summaries...")
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

# Word cloud records (top 30 words)
for word, count in word_counts.most_common(30):
    summaries.append({'attributes': {
        'geo_type': 'WordCloud',
        'geo_name': word,
        'parent_name': '',
        'activity_count': count,
        'individual_count': 0,
        'last_updated': now_ms
    }})
print(f"WordCloud records: {min(30, len(word_counts))}")

# Individual records
individual_counts = activities_sdf.groupby('_individual').size().to_dict()
for name, count in individual_counts.items():
    display_name = str(name).strip() if name else "Anonymous"
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

print("\n[6/7] Updating summary table...")
existing = summary_layer.query(where="1=1", return_geometry=False, out_fields="*")
if existing.features:
    oid_field = [k for k in existing.features[0].attributes.keys() if 'objectid' in k.lower()][0]
    oids = [f.attributes[oid_field] for f in existing.features]
    summary_layer.edit_features(deletes=oids)
    print(f"Deleted {len(oids)} old records")

print("[7/7] Adding new records...")
result = summary_layer.edit_features(adds=summaries)
print(f"✅ DONE! Added {sum(1 for r in result['addResults'] if r['success'])} records.")
print(f"   Total individuals: {total_individuals}")
print(f"   Word cloud words: {min(30, len(word_counts))}")

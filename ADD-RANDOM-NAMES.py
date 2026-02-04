"""
Add random diverse names to existing records for demo purposes.
Run this ONCE to populate first_name and last_name fields.
"""

from arcgis.gis import GIS
from arcgis.features import FeatureLayer
import random

gis = GIS("home")
print(f"Connected as: {gis.users.me.username}")

layer = FeatureLayer("https://services.arcgis.com/pGfbNJoYypmNq86F/arcgis/rest/services/Community_Discovery_Visualization/FeatureServer/0")

# Diverse name lists
FIRST_NAMES = [
    # Female names - diverse
    "Maria", "Aisha", "Jennifer", "Mei-Lin", "Priya", "Fatima", "Sarah", "Yuki",
    "Olga", "Esperanza", "Lakshmi", "Keisha", "Nguyen", "Isabella", "Amara",
    "Svetlana", "Aminata", "Rosa", "Hana", "Destiny",
    # Male names - diverse
    "James", "Mohammed", "Carlos", "Wei", "Jamal", "Dmitri", "Kofi", "Raj",
    "Miguel", "Kwame", "Hiroshi", "Omar", "Alejandro", "Darnell", "Sanjay",
    "Ivan", "Tariq", "Luis", "Jin", "Marcus"
]

LAST_NAMES = [
    # Diverse surnames
    "Smith", "Garcia", "Patel", "Kim", "Nguyen", "Johnson", "Williams", "Chen",
    "Martinez", "Brown", "Davis", "Rodriguez", "Wilson", "Anderson", "Thomas",
    "Jackson", "Lee", "Hernandez", "Moore", "Taylor", "White", "Harris",
    "Washington", "Singh", "Okonkwo", "Nakamura", "Petrov", "Santos", "Ali",
    "Cohen", "Murphy", "Gonzalez", "Rivera", "Campbell", "Mitchell", "Roberts"
]

# Query all records
print("Fetching records...")
features = layer.query(where="1=1", out_fields="*", return_geometry=False)
print(f"Found {len(features.features)} records")

# Assign random names to each record
updates = []
for feature in features.features:
    oid = feature.attributes['objectid']
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)

    updates.append({
        'attributes': {
            'objectid': oid,
            'first_name': first,
            'last_name': last
        }
    })
    print(f"  {oid}: {first} {last}")

# Update in batches of 100
print(f"\nUpdating {len(updates)} records...")
batch_size = 100
success_count = 0

for i in range(0, len(updates), batch_size):
    batch = updates[i:i+batch_size]
    result = layer.edit_features(updates=batch)
    success_count += sum(1 for r in result['updateResults'] if r['success'])
    print(f"  Batch {i//batch_size + 1}: {len(batch)} records")

print(f"\n✅ DONE! Updated {success_count} records with random names.")
print("Now run the main notebook (NOTEBOOK-WITH-WORDCLOUD.py) to regenerate summaries.")

import streamlit as st
import requests
import sqlite3
import pandas as pd

# --- Config ---
API_KEY = "bbab9da0-4313-4c3e-9034-7a1c88187dd6"
DB_NAME = "hv.db"  # Use the same database file

# --- DB Setup: Create tables if not exist ---
conn = sqlite3.connect(DB_NAME)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS artifact_metadata (
    id INTEGER PRIMARY KEY,
    title TEXT,
    culture TEXT,
    period TEXT,
    century TEXT,
    medium TEXT,
    dimensions TEXT,
    description TEXT,
    department TEXT,
    classification TEXT,
    accessionyear streamlitINTEGER,
    accessionmethod TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS artifact_media (
    objectid INTEGER PRIMARY KEY,
    imagecount INTEGER,
    mediacount INTEGER,
    colorcount INTEGER,
    rank REAL,
    datebegin INTEGER,
    dateend INTEGER,
    FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS artifact_colors (
    objectid INTEGER,
    color TEXT,
    spectrum TEXT,
    hue TEXT,
    percent REAL,
    css3 TEXT,
    FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
)
''')

conn.commit()
conn.close()

# --- Streamlit UI ---
st.title("🏺 Harvard Artifacts Collector")

classifications = ['Coins', 'Paintings', 'Drawings', 'Vessels', 'Sculpture']
selected_classification = st.selectbox("Choose a classification", classifications)

# To hold collected data temporarily
if "metadata_list" not in st.session_state:
    st.session_state.metadata_list = []
    st.session_state.media_list = []
    st.session_state.colors_list = []

# --- Collect Data ---
if st.button("Collect Data"):
    st.session_state.metadata_list, st.session_state.media_list, st.session_state.colors_list = [], [], []

    with st.spinner("Fetching data..."):
        all_records = []
        for page in range(1, 26):  # 25 pages × 100 = 2500 records
            url = "https://api.harvardartmuseums.org/object"
            params = {
                "apikey": API_KEY,
                "classification": selected_classification,
                "size": 100,
                "page": page
            }
            res = requests.get(url, params=params)
            if res.status_code != 200:
                st.error(f"API error: {res.status_code}")
                break

            page_records = res.json().get("records", [])
            if not page_records:
                break
            all_records.extend(page_records)

        # Process all collected records
        for record in all_records:
            meta = {
                "id": record.get("id"),
                "title": record.get("title"),
                "culture": record.get("culture"),
                "period": record.get("period"),
                "century": record.get("century"),
                "medium": record.get("medium"),
                "dimensions": record.get("dimensions"),
                "description": record.get("description"),
                "department": record.get("department"),
                "classification": record.get("classification"),
                "accessionyear": record.get("accessionyear"),
                "accessionmethod": record.get("accessionmethod")
            }
            st.session_state.metadata_list.append(meta)

            media = {
                "objectid": record.get("id"),
                "imagecount": record.get("imagecount"),
                "mediacount": record.get("mediacount"),
                "colorcount": record.get("colorcount"),
                "rank": record.get("rank"),
                "datebegin": record.get("datebegin"),
                "dateend": record.get("dateend"),
            }
            st.session_state.media_list.append(media)

            if record.get("colors"):
                for c in record["colors"]:
                    color = {
                        "objectid": record.get("id"),
                        "color": c.get("color"),
                        "spectrum": c.get("spectrum"),
                        "hue": c.get("hue"),
                        "percent": c.get("percent"),
                        "css3": c.get("css3")
                    }
                    st.session_state.colors_list.append(color)

    st.success(f"✅ Collected {len(st.session_state.metadata_list)} {selected_classification} records.")

    # Show collected data preview
    col1, col2, col3 = st.columns(3)
    with col1: st.subheader("📜 Metadata"); st.json(st.session_state.metadata_list[:3])
    with col2: st.subheader("🖼 Media"); st.json(st.session_state.media_list[:3])
    with col3: st.subheader("🎨 Colors"); st.json(st.session_state.colors_list[:3])

# --- Migrate Data ---
if st.button("Migrate Data"):
    if not st.session_state.metadata_list:
        st.warning("⚠️ No collected data. Please collect first.")
    else:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        records = [
    (
        art['id'], art['title'], art['culture'], art.get('period'),
        art['century'],art.get('medium'), art['dimensions'],art['description'],art['department'], art['classification'], 
        art['accessionyear'], art['accessionmethod']
    
    )
    for art in st.session_state.metadata_list
        ]

        
        cursor.executemany("""
            INSERT OR IGNORE INTO artifact_metadata (
                id, title, culture, period, century, medium,
                dimensions, description, department,
                classification, accessionyear, accessionmethod
            ) VALUES (
                ?, ?, ?, ?, ?,?,
                ?, ?, ?,
                ?, ?, ?
            )
        """,records )
        media_records = [
    (m['objectid'], m['imagecount'], m['mediacount'], m['colorcount'],
     m['rank'], m['datebegin'], m['dateend'])
    for m in st.session_state.media_list
      ]


        cursor.executemany("""
            INSERT OR IGNORE INTO artifact_media (
                objectid, imagecount, mediacount, colorcount,
                rank, datebegin, dateend
            ) VALUES (
                ?, ?, ?, ?,?,?,?

            )
        """, media_records )
        color_tuples = [
    (record['objectid'],record['color'], record['spectrum'], record['hue'], record['percent'], record['css3'])
    for record in st.session_state.colors_list 
]

        cursor.executemany("""
            INSERT INTO artifact_colors (
                objectid, color, spectrum, hue, percent, css3
            ) VALUES (
                ?, ?, ?, ?, ?, ?
            )
        """,color_tuples  )

        conn.commit()

        # Show migrated data
        meta_df = pd.read_sql("SELECT * FROM artifact_metadata", conn)
        media_df = pd.read_sql("SELECT * FROM artifact_media", conn)
        color_df = pd.read_sql("SELECT * FROM artifact_colors", conn)
        conn.close()

        st.success("✅ Data migrated successfully!")
        tab1, tab2, tab3 = st.tabs([" Metadata ", " Media ", " Colors "])
        with tab1: st.dataframe(meta_df)
        with tab2: st.dataframe(media_df)
        with tab3: st.dataframe(color_df)

# --- SQL Query Explorer ---
st.header("🔍 SQL Query Explorer")
questions_queries = {
    "1. List all artifacts from the 11th century belonging to Byzantine culture.":
        "SELECT * FROM artifact_metadata WHERE century LIKE '%11%' AND culture='Byzantine';",

    "2. What are the unique cultures represented in the artifacts?":
       """
        SELECT DISTINCT culture 
        FROM artifact_metadata 
        WHERE culture IS NOT NULL;
        """,

    "3. List all artifacts from the Archaic Period.":
        "SELECT * FROM artifact_metadata WHERE period LIKE '%Archaic%';",

    "4. List artifact titles ordered by accession year in descending order.":
        "SELECT title, accessionyear FROM artifact_metadata ORDER BY accessionyear DESC;",

    "5. How many artifacts are there per department?":
        "SELECT department, COUNT(*) AS count FROM artifact_metadata GROUP BY department;",

    "6. Which artifacts have more than 1 image?":
        "SELECT * FROM artifact_media WHERE imagecount > 1;",

    "7. What is the average rank of all artifacts?":
        "SELECT AVG(rank) AS average_rank FROM artifact_media;",

    "8. Which artifacts have a higher colorcount than mediacount?":
        "SELECT * FROM artifact_media WHERE colorcount > mediacount;",

    "9. List all artifacts created between 1500 and 1600.":
        "SELECT * FROM artifact_metadata WHERE accessionyear BETWEEN 1500 AND 1600;",

    "10. How many artifacts have no media files?":
        "SELECT COUNT(*) FROM artifact_media WHERE mediacount = 0;",

    "11. What are all the distinct hues used in the dataset?":
        "SELECT DISTINCT hue FROM artifact_colors WHERE hue IS NOT NULL;",

    "12. What are the top 5 most used colors by frequency?":
        "SELECT color, COUNT(*) as frequency FROM artifact_colors GROUP BY color ORDER BY frequency DESC LIMIT 5;",

    "13. What is the average coverage percentage for each hue?":
        "SELECT hue, AVG(percent) as avg_coverage FROM artifact_colors GROUP BY hue;",

    "14. List all colors used for a given artifact ID .":
        "SELECT objectid, color, spectrum, hue, percent, css3 FROM artifact_colors WHERE objectid;",
 
    "15. What is the total number of color entries in the dataset?":
        "SELECT COUNT(*) as total_colors FROM artifact_colors;",

    "16. List artifact titles and hues for all artifacts belonging to the Byzantine culture.":
        """
        SELECT a.title, c.hue 
        FROM artifact_metadata a 
        JOIN artifact_colors c ON a.id = c.objectid 
        WHERE a.culture = 'Byzantine';
        """,

    "17. List each artifact title with its associated hues.":
        """
        SELECT a.title, c.hue 
        FROM artifact_metadata a 
        JOIN artifact_colors c ON a.id = c.objectid;
        """,

    "18. Get artifact titles, cultures, and media ranks where the period is not null.":
        """
        SELECT a.title, a.culture, m.rank 
        FROM artifact_metadata a 
        JOIN artifact_media m ON a.id = m.objectid 
        WHERE a.period IS NOT NULL;
        """,

    "19. Find artifact titles ranked in the top 10 that include the color hue 'Grey'.":
        """
        SELECT DISTINCT a.title 
        FROM artifact_metadata a 
        JOIN artifact_media m ON a.id = m.objectid 
        JOIN artifact_colors c ON m.objectid = c.objectid 
        WHERE c.hue = 'Grey' 
        ORDER BY m.rank DESC 
        LIMIT 10;
        """,

    "20. How many artifacts exist per classification, and what is the average media count for each?":
        """
        SELECT a.classification, COUNT(*) as count, AVG(m.mediacount) as avg_media_count 
        FROM artifact_metadata a 
        JOIN artifact_media m ON a.id = m.objectid 
        GROUP BY a.classification;
        """,
    "21. List all artifacts in the database along with their ID, title, and culture?":

        """
        SELECT id, title, culture
        FROM artifact_metadata;
        """,
 

   
     "22. List artifacts with media rank greater than 50.":
        """
        SELECT objectid, rank
        FROM artifact_media
        WHERE rank > 50
        ORDER BY rank DESC;
        """,
    "23.Count how many artifacts contain the color hue 'Green'":

        """
        SELECT COUNT(DISTINCT objectid) AS a_green
        FROM artifact_colors
        WHERE hue = 'Green';
       
        """,
     "24.Top 5 hues used in artifacts classified as 'Sculpture':":
        """
        SELECT c.hue, COUNT(*) AS usage_count
        FROM artifact_metadata m
        JOIN artifact_colors c ON m.id = c.objectid
        WHERE m.classification = 'Sculpture'
        GROUP BY c.hue
        ORDER BY usage_count DESC
        LIMIT 5;
        
        """,
    "25. Find artifacts from the 16th century with rank > 50 and color hue 'Red'":
        """
        SELECT m.title, me.rank, c.hue
        FROM artifact_metadata m
        JOIN artifact_media me ON m.id = me.objectid
        JOIN artifact_colors c ON m.id = c.objectid
        WHERE m.century = '16th century'
        AND me.rank > 50
        AND c.hue = 'Red';
        """,

}

selected_question = st.selectbox("Select a SQL query to execute:", [""] + list(questions_queries.keys()))
if selected_question:
    query = questions_queries[selected_question]
    try:
        conn = sqlite3.connect(DB_NAME)
        df = pd.read_sql_query(query, conn)
        conn.close()
        st.subheader("📊 Query Result")
        st.dataframe(df)
    except Exception as e:
        st.error(f"SQL query failed: {e}")

# --- Clear Data ---
if st.button("🗑️ Clear Data"):
    if st.session_state.get("confirm_clear", False):
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM artifact_metadata;")
        cursor.execute("DELETE FROM artifact_media;")
        cursor.execute("DELETE FROM artifact_colors;")
        conn.commit()
        cursor.execute("VACUUM;")
        conn.close()

        st.session_state.metadata_list = []
        st.session_state.media_list = []
        st.session_state.colors_list = []
        st.session_state.confirm_clear = False

        st.success("✅ All data cleared.")
        st.rerun()
    else:
        st.session_state.confirm_clear = True
        st.warning("⚠️ Click **Clear Data** again to confirm deletion.")

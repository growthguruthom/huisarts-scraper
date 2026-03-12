"""Streamlit dashboard for Huisarts Verbouwing Scraper."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from scraper.db import init_db, get_dashboard_data, get_unmatched_signalen, get_stats

st.set_page_config(
    page_title="Huisarts Verbouwing Tracker",
    page_icon="🏥",
    layout="wide",
)

# Initialize DB
init_db()


def main():
    st.title("Huisarts Verbouwing Tracker")
    st.caption("Identificeert huisartspraktijken die gaan verbouwen in Nederland")

    # Stats
    stats = get_stats()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Praktijken", stats["praktijken"])
    col2.metric("Signalen", stats["signalen"])
    col3.metric("Matches", stats["matches"])
    col4.metric("Gemeenten", stats["gemeenten"])

    if stats["laatste_update"]:
        st.caption(f"Laatste update: {stats['laatste_update'][:19]}")

    if stats["matches"] == 0 and stats["signalen"] == 0:
        st.info(
            "Nog geen data. Run de scraper eerst:\n\n"
            "```bash\n"
            "cd ~/Desktop/huisarts-scraper\n"
            "source .venv/bin/activate\n"
            "python main.py --refresh-practices  # Eenmalig: praktijken ophalen\n"
            "python main.py                      # Signalen ophalen + matchen\n"
            "```"
        )
        return

    # Load data
    matches_data = get_dashboard_data()
    unmatched_data = get_unmatched_signalen()

    tab1, tab2, tab3 = st.tabs(["Overzicht", "Kaart", "Ongematchte signalen"])

    with tab1:
        _render_overview(matches_data)

    with tab2:
        _render_map(matches_data)

    with tab3:
        _render_unmatched(unmatched_data)


def _render_overview(data: list[dict]):
    """Render the main overview table with filters."""
    if not data:
        st.warning("Nog geen matches gevonden.")
        return

    df = pd.DataFrame(data)

    # Sidebar filters
    st.sidebar.header("Filters")

    # Signal type filter
    signal_types = df["signaal_type"].dropna().unique().tolist()
    selected_types = st.sidebar.multiselect(
        "Signaaltype",
        options=signal_types,
        default=signal_types,
    )

    # Match score filter
    scores = df["match_score"].dropna().unique().tolist()
    selected_scores = st.sidebar.multiselect(
        "Match score",
        options=scores,
        default=scores,
    )

    # City filter
    cities = sorted(df["praktijk_stad"].dropna().unique().tolist())
    selected_cities = st.sidebar.multiselect("Stad", options=cities)

    # Apply filters
    mask = df["signaal_type"].isin(selected_types) & df["match_score"].isin(selected_scores)
    if selected_cities:
        mask = mask & df["praktijk_stad"].isin(selected_cities)
    filtered = df[mask]

    st.subheader(f"Resultaten ({len(filtered)} van {len(df)})")

    # Display table
    display_cols = [
        "praktijk_naam", "praktijk_stad", "praktijk_postcode",
        "signaal_type", "signaal_titel", "publicatiedatum",
        "match_score", "gemeente", "contact_naam",
    ]
    display_cols = [c for c in display_cols if c in filtered.columns]

    st.dataframe(
        filtered[display_cols].rename(columns={
            "praktijk_naam": "Praktijk",
            "praktijk_stad": "Stad",
            "praktijk_postcode": "Postcode",
            "signaal_type": "Type",
            "signaal_titel": "Signaal",
            "publicatiedatum": "Datum",
            "match_score": "Score",
            "gemeente": "Gemeente",
            "contact_naam": "Contact",
        }),
        use_container_width=True,
        hide_index=True,
    )

    # Detail view
    if not filtered.empty:
        st.subheader("Detail")
        selected_idx = st.selectbox(
            "Selecteer een match voor details",
            options=filtered.index.tolist(),
            format_func=lambda i: f"{filtered.loc[i, 'praktijk_naam']} - {filtered.loc[i, 'signaal_titel'][:60]}",
        )

        if selected_idx is not None:
            row = filtered.loc[selected_idx]
            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Praktijk**")
                st.write(f"**Naam:** {row.get('praktijk_naam', '-')}")
                st.write(f"**AGB-code:** {row.get('agb_code', '-')}")
                st.write(f"**Adres:** {row.get('praktijk_adres', '-')}")
                st.write(f"**Postcode:** {row.get('praktijk_postcode', '-')} {row.get('praktijk_stad', '-')}")
                if row.get("telefoon"):
                    st.write(f"**Telefoon:** {row['telefoon']}")
                if row.get("website"):
                    st.write(f"**Website:** [{row['website']}]({row['website']})")

            with col2:
                st.markdown("**Signaal**")
                st.write(f"**Type:** {row.get('signaal_type', '-')}")
                st.write(f"**Titel:** {row.get('signaal_titel', '-')}")
                st.write(f"**Datum:** {row.get('publicatiedatum', '-')}")
                st.write(f"**Gemeente:** {row.get('gemeente', '-')}")
                st.write(f"**Match:** {row.get('match_score', '-')} ({row.get('match_type', '-')})")
                if row.get("omschrijving"):
                    st.write(f"**Omschrijving:** {row['omschrijving'][:300]}")
                if row.get("bron_url"):
                    st.markdown(f"[Bekijk bron]({row['bron_url']})")

            # Research section
            has_research = row.get("contact_naam") or row.get("nieuws_titel")
            if has_research:
                st.divider()
                col3, col4 = st.columns(2)
                with col3:
                    st.markdown("**Contactpersoon**")
                    if row.get("contact_naam"):
                        st.write(f"**Naam:** {row['contact_naam']}")
                        st.write(f"**Rol:** {row.get('contact_rol', '-')}")
                        if row.get("contact_bron"):
                            st.markdown(f"[Bron]({row['contact_bron']})")
                    else:
                        st.write("Geen contactpersoon gevonden")
                with col4:
                    st.markdown("**Nieuws / Haakje**")
                    if row.get("nieuws_titel"):
                        st.write(f"**Titel:** {row['nieuws_titel']}")
                        if row.get("nieuws_samenvatting"):
                            st.write(f"**Samenvatting:** {row['nieuws_samenvatting']}")
                        if row.get("nieuws_url"):
                            st.markdown(f"[Lees artikel]({row['nieuws_url']})")
                    else:
                        st.write("Geen nieuwsartikel gevonden")


def _render_map(data: list[dict]):
    """Render a map of matched practices."""
    if not data:
        st.warning("Geen data voor kaartweergave.")
        return

    df = pd.DataFrame(data)
    map_data = df.dropna(subset=["lat", "lon"])

    if map_data.empty:
        st.warning("Geen coördinaten beschikbaar. Praktijken van ZorgkaartNederland bevatten GPS-data.")
        return

    # Create Folium map centered on Netherlands
    m = folium.Map(location=[52.1326, 5.2913], zoom_start=7)

    # Add markers
    for _, row in map_data.iterrows():
        popup_html = f"""
        <b>{row['praktijk_naam']}</b><br>
        {row.get('praktijk_adres', '')}<br>
        {row.get('praktijk_postcode', '')} {row.get('praktijk_stad', '')}<br>
        <br>
        <b>Signaal:</b> {row.get('signaal_type', '')}<br>
        {row.get('signaal_titel', '')[:100]}<br>
        <a href="{row.get('bron_url', '#')}" target="_blank">Bekijk bron</a>
        """

        color = {
            "hoog": "red",
            "medium": "orange",
            "laag": "blue",
        }.get(row.get("match_score"), "gray")

        folium.Marker(
            location=[row["lat"], row["lon"]],
            popup=folium.Popup(popup_html, max_width=300),
            tooltip=row["praktijk_naam"],
            icon=folium.Icon(color=color, icon="plus", prefix="fa"),
        ).add_to(m)

    st_folium(m, width=None, height=600)

    st.caption("Rood = hoog vertrouwen, Oranje = medium, Blauw = laag")


def _render_unmatched(data: list[dict]):
    """Render unmatched signalen."""
    if not data:
        st.success("Alle signalen zijn gematcht met een praktijk.")
        return

    st.subheader(f"Ongematchte signalen ({len(data)})")
    st.caption("Signalen die niet automatisch gekoppeld konden worden aan een praktijk.")

    df = pd.DataFrame(data)
    display_cols = ["type", "titel", "gemeente", "stad", "postcode", "publicatiedatum", "bron_url"]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols].rename(columns={
            "type": "Type",
            "titel": "Titel",
            "gemeente": "Gemeente",
            "stad": "Stad",
            "postcode": "Postcode",
            "publicatiedatum": "Datum",
            "bron_url": "Bron",
        }),
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    main()

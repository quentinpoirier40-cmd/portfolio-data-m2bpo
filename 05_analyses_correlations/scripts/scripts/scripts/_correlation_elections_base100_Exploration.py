def tracer_analyse_finale(data, dimension, col_phase, titre, seuil_annonces=3000):
    # 1. On identifie les segments qui pèsent vraiment (ex: > 3000 lignes)
    counts = data[dimension].value_counts()
    gros_segments = counts[counts >= seuil_annonces].index.tolist()
    
    # 2. On filtre le dataframe pour le graphique
    df_plot = data[data[dimension].isin(gros_segments)].copy()
    
    # 3. Création de la Base 100 (S-4 = 100)
    ordre_phases = ['S-4', 'S-3', 'S-2', 'S-1', 'S0', 'S+1', 'S+2', 'S+3', 'S+4']
    # On garde que les lignes de l'élection choisie
    data_cycle = df_plot[df_plot[col_phase].notna()].copy()
    
    pivot = data_cycle.groupby([dimension, col_phase]).size().unstack(fill_value=0)
    pivot = pivot.reindex(columns=ordre_phases, fill_value=0)
    
    # Calcul de la variation interne (Indice de maintien)
    variation = pivot.div(pivot['S-4'].replace(0, 1), axis=0) * 100
    
    # 4. Affichage
    plt.figure(figsize=(15, 8))
    sns.heatmap(variation, annot=True, fmt=".0f", cmap="RdYlGn", center=100)
    plt.title(f"{titre} : Indice de maintien de l'activité par {dimension}\n(Base 100 en S-4)")
    plt.show()

# Exemple d'appel pour Blaise
tracer_analyse_finale(df, 'MO_type_', 'phase_muni_2020', 'Municipales 2020', seuil_annonces=3000)

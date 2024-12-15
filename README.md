# Pipeline de Données de Mobilité des Bornes de Vélos en Temps Réel

Ce projet traite des données d'utilisation des bornes de vélos en libre-service, disponibles en open source et en temps réel, dans les grandes villes de France. Le pipeline collecte, consolide et agrège ces données afin de faciliter leur analyse pour des besoins tels que l'optimisation des infrastructures de mobilité urbaine ou l'analyse des comportements d'utilisation.

## Table des Matières

1. [Structure du Projet](#structure-du-projet)
2. [Utilisation](#utilisation)
3. [Description des Fichiers](#description-des-fichiers)
4. [Contributeurs](#contributeurs)

## Structure du Projet

Le pipeline se décompose en trois étapes principales :

1. **Ingestion des Données** :
   - Récupère en temps réel les données open source des bornes de vélos en libre-service de différentes villes françaises (Paris, Nantes, Toulouse).
   
2. **Consolidation des Données** :
   - Organise les données ingérées dans des tables centralisées.
   - Garantit l'uniformité des données provenant de différentes villes et formats.
   
3. **Agrégation des Données** :
   - Structure les données en tables analytiques prêtes à l’emploi, telles que des tables dimensionnelles.

## Utilisation

1. Clonez le dépôt :
   ```bash
   git clone https://github.com/kevinl75/polytech-de-101-2024-tp-subject.git  
   cd polytech-de-101-2024-tp-subject
   python3 -m venv .venv
   source .venv/bin/activate

2. Installez les dépendances :
    ```bash
    pip install -r requirements.txt

3. Pour exécuter le projet :
   ```bash
   python src/main.py


Le script exécutera les étapes suivantes :

- Ingestion des données : Télécharge les données en temps réel.
- Consolidation des données : Organise les données pour une cohérence et une facilité d’analyse.
- Agrégation des données : Crée des tables analytiques optimisées pour l'analyse, en suivant une modélisation dimensionnelle. Les résultats seront stockés dans une base de données DuckDB, offrant ainsi une exploration rapide et efficace des données.

## Description des Fichiers

1. **data_ingestion.py**

Ce fichier contient les fonctions pour télécharger les données open source des bornes de vélos en temps réel :
- get_paris_realtime_bicycle_data() : Récupère les données pour les stations de Paris.
- get_nantes_realtime_bicycle_data() : Récupère les données pour les stations de Nantes.
- get_toulouse_realtime_bicycle_data() : Récupère les données pour les stations de Toulouse.
- get_commune_data() : Récupère des informations générales sur les communes.

2. **data_consolidation.py**

Ce fichier traite la normalisation et l’organisation des données ingérées :

- create_consolidate_tables() : Crée les tables de consolidation dans la base de données en exécutant des requêtes SQL à partir d'un fichier.
- consolidate_station_data() : Consolide les données en temps réel des stations de vélos pour Paris, Nantes et Toulouse et les insère dans la table CONSOLIDATE_STATION de DuckDB.
- consolidate_city_data() : Consolide les données des villes (Paris, Nantes, Toulouse) à partir d'un fichier JSON et les insère dans la table CONSOLIDATE_CITY.
- consolidate_station_statement_data() : Consolide les données des déclarations de stations de vélos pour Paris, Nantes et Toulouse et les insère dans la table CONSOLIDATE_STATION_STATEMENT.

3. **data_agregation.py**

- create_agregate_tables() : Crée les tables destinées à l’agrégation.
- agregate_dim_city() : Agrège les données de la table DIM_CITY en insérant ou remplaçant les informations les plus récentes provenant de CONSOLIDATE_CITY.
- agregate_dim_station() : Agrège les données de la table DIM_STATION en insérant ou remplaçant les informations les plus récentes provenant de CONSOLIDATE_STATION.
- agregate_fact_station_statements() : Crée une table factuelle contenant les statistiques des bornes de vélos.

4. **main.py**

Le point d’entrée principal du projet. Ce fichier orchestre les étapes d’ingestion, de consolidation et d’agrégation des données.

## Contributeurs

HROUMA Samia - [Profil GitHub](https://github.com/SamHrm)
HROUMA Salma - [Profil GitHub](https://github.com/salmahrouma)

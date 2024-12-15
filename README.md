# Pipeline de Données de Mobilité des Bornes de Vélos en Temps Réel

Ce projet traite des données d'utilisation des bornes de vélos en libre-service, disponibles en open source et en temps réel, dans les grandes villes de France. Le pipeline collecte, consolide et agrège ces données afin de faciliter leur analyse pour des besoins tels que l'optimisation des infrastructures de mobilité urbaine ou l'analyse des comportements d'utilisation.

## Table des Matières

1. [Structure du Projet](#structure-du-projet)
2. [Installation](#installation)
3. [Utilisation](#utilisation)
4. [Description des Fichiers](#description-des-fichiers)
5. [Contributeurs](#contributeurs)

## Structure du Projet

Le pipeline se décompose en trois étapes principales :

1. **Ingestion des Données** :
   - Récupère en temps réel les données open source des bornes de vélos en libre-service de différentes villes françaises (Paris, Nantes, Toulouse).
   
2. **Consolidation des Données** :
   - Organise les données ingérées dans des tables centralisées.
   - Garantit l'uniformité des données provenant de différentes villes et formats.
   
3. **Agrégation des Données** :
   - Structure les données en tables analytiques prêtes à l’emploi, telles que des tables dimensionnelles.

## Installation

1. Clonez le dépôt :
   ```bash
   git clone https://github.com/kevinl75/polytech-de-101-2024-tp-subject.git  
   cd polytech-de-101-2024-tp-subject
   python3 -m venv .venv
   source .venv/bin/activate
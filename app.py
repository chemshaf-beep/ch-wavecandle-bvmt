import shutil
import tempfile
import os, time
os.environ["TZ"] = "Africa/Tunis"
if hasattr(time, "tzset"):
    try: time.tzset()
    except Exception: pass
import gzip
import os, time
os.environ['TZ'] = 'Africa/Tunis'
try:
    time.tzset()
except Exception:
    pass
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Version Révisée 100% Découplée - Conforme Système Atos NSC / Bourse de Tunis
# # PARTIE 1 / 20 : ADRESSAGE DYNAMIQUE SANS IMPOSITION DE LETTRE DISQUE
# ==============================================================================
import os
import sys
import time
import datetime
import re
import json
import warnings
import requests
import pandas as pd
import numpy as np
import signal
import threading  

# Interdiction passive des avertissements SSL liés aux infrastructures Falcon
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Détection de l argument d appel pour aiguiller l infrastructure de session
MODE_SIMULATION_ACTIF = False
if len(sys.argv) > 1 and str(sys.argv[1]).strip().lower() in ("simu", "simulation"):
    MODE_SIMULATION_ACTIF = True
    
MODE_PRODUCTION = not MODE_SIMULATION_ACTIF

# 🎯 DECOUPLAGE LOGIQUES : Résolution dynamique des dossiers (0% Conflit de lettre R:)
DOSSIER_SCRIPT = os.path.dirname(os.path.abspath(__file__)) 
REPERTOIRE_HISTO = os.path.join(DOSSIER_SCRIPT, "Historique") 

# Routage chirurgical du cache RAM volatile selon le système d exploitation hôte
if sys.platform.startswith("win"):
    REPERTOIRE_JSON = os.path.join(os.getenv("TEMP", DOSSIER_SCRIPT), "Bourse_MACSA_Cache")
else:
    REPERTOIRE_JSON = "/dev/shm/Bourse_MACSA_Cache"

# Armement physique et sécurisé des coques de stockage local
os.makedirs(REPERTOIRE_HISTO, exist_ok=True) 
os.makedirs(REPERTOIRE_JSON, exist_ok=True) 
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Configuration Géométrique des Flux Glissants
# # PARTIE 2 / 20 : ETALONNAGE DES ANCRAGES ET SEGMENTS DISQUES [ALIGNEMENT DOSSIERS]
# ==============================================================================
DATE_JOUR_STR = datetime.date.today().strftime("%d%m%Y") 

# Spécifications des endpoints d infrastructures Falcon et fichiers sources de simulation
URL_API = "https://online.macsa.com.tn:8443/marketApi/sbe/market-report/11,12,13,32,33,99"
FICHIER_SIMULE_SOURCE = os.path.join(DOSSIER_SCRIPT, "SbeMarket-Data.csv") 

# Matrice globale à plat lue par l ADODB d Excel toutes les 500 ms
FICHIER_TEXTE_GRILLE = os.path.join(REPERTOIRE_JSON, "DonneesSource.txt") 

# 🎯 ARCHITECTURE DE ROUTAGE ABSOLUE SCELLÉE CONFORME À LA GRILLE PHYSIQUE DE PLACE
REPERTOIRE_HISTO_REEL = os.path.join(REPERTOIRE_HISTO, "bourse_historique_reel")
REPERTOIRE_SYNTHESE = os.path.join(REPERTOIRE_HISTO, "Synthese_cloture")

# Armement matériel des coques de sous-répertoires requis (0% Erreur ntpath)
os.makedirs(REPERTOIRE_HISTO_REEL, exist_ok=True)
os.makedirs(REPERTOIRE_SYNTHESE, exist_ok=True)

# Nomenclature glissante des journaux d activité micro-structurelle HFT
PREFIXE_LOG = "bourse_historique_reel_" if MODE_PRODUCTION else "bourse_historique_simulation_" 
FICHIER_HISTO_RAMDISK = os.path.join(REPERTOIRE_JSON, f"temp_{PREFIXE_LOG}{DATE_JOUR_STR}.csv") 

# Aiguillage du fichier journal permanent de séance (.csv) vers le sous-dossier dédié
FICHIER_HISTO_SEANCE = os.path.join(REPERTOIRE_HISTO_REEL, f"{PREFIXE_LOG}{DATE_JOUR_STR}.csv") 

if MODE_PRODUCTION:
    # Ingestion unifiée vers le répertoire Synthese_cloture
    FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_reel_{DATE_JOUR_STR}.csv")
    FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_reel_{DATE_JOUR_STR}.json")
else:
    FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_simulation_{DATE_JOUR_STR}.csv")
    FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_simulation_{DATE_JOUR_STR}.json")

# Coques d interconnectivités asynchrones inter-thread (Appellations d Origine Préservées)
BUFFER_TEXTE_RESEAU_FALCON = ""
VERROU_PARTAGE_THREAD = threading.Lock()
DATE_SEANCE_EN_COURS = datetime.date.today().strftime("%d%m%Y")
EST_ARCHIVE_FINALE_ENGAGEE = False
df_master_ram_cote = None

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Spécifications des Grilles de Session
# # PARTIE 3 / 20 : SCELLAGE COMPTABLE DES EN-TÊTES MATRIX
# ==============================================================================
# En-têtes fixes de la Grille unifiée DonneesSource.txt (40 colonnes physiques)
LISTE_ENTETES_OFFICIELS = [
    "SbeMarket SheetValeur", "Etat", "Qt. A", "Achat", "Vente", "Qt.V", "Clôture", "CTO", "VTO", "Ouverture",
    "Dernier", "Var.", "CMP", "Qté", "Volume", "P. Haut", "P. Bas", "Seuil.S. Haut", "Seuil.S. Bas", "Seuil.D. Haut",
    "Seuil.D. Bas", "Heure"
]

# Nomenclature unifiée à 42 colonnes pour l extraction chronologique du grand livre CSV
ENTETES_HISTORIQUE_CSV = [
    "Ticker", "Heure_Marche", "Heure_Python", "Nature_Operation", "Delta_Qte", "Prix_Execution", "Delta_Vol",
    "T_Phase_Reglementaire", "T_Alerte_Volatilis", "T_Alerte_Seuil", "T_Graph_Flash", "T0_CMP", "T0_Etat", "T0_Qt_A",
    "T0_Achat", "T0_Vente", "T0_Qt_V", "T0_Cloture", "T0_CTO", "T0_VTO", "T0_Ouverture", "T0_Dernier", "T0_Var",
    "T0_CMP_Brut", "T0_Qte_Seance", "T0_Volume_Seance", "T0_Heure_Marche", "T1_Etat", "T1_Qt_A", "T1_Achat", "T1_Vente",
    "T1_Qt_V", "T1_Cloture", "T1_CTO", "T1_VTO", "T1_Ouverture", "T1_Dernier", "T1_Var", "T1_CMP", "T1_Qte_Seance",
    "T1_Volume_Seance", "Date_Seance"
]

# Allocation des registres de transition glissante en mémoire vive
cumul_qte_achat, cumul_vol_achat = {}, {}
cumul_qte_vente, cumul_vol_vente = {}, {}
cumul_qte_indecis, cumul_vol_indecis = {}, {}
historique_volumes_qte, historique_volumes_montant = {}, {}
historique_dernier_cours, historique_phase_precedente = {}, {}


# ==============================================================================
# PARTIE 4 / 20 : NETTOYAGE DES FICHIERS DE LA VEILLE ET RE-INITIALISATION
# ==============================================================================
def verifier_et_nettoyer_fichiers_veille(date_jour_actuelle):
    global REPERTOIRE_JSON, PREFIXE_LOG, REPERTOIRE_HISTO_REEL, FICHIER_TEXTE_GRILLE
    try:
        if not os.path.exists(REPERTOIRE_JSON):
            return

        for nom_f in os.listdir(REPERTOIRE_JSON):
            chemin_complet = os.path.join(REPERTOIRE_JSON, nom_f)
            if not os.path.isfile(chemin_complet):
                continue

            if nom_f == os.path.basename(FICHIER_TEXTE_GRILLE):
                continue

            if date_jour_actuelle not in nom_f:
                if nom_f.startswith("temp_bourse_historique_") and nom_f.endswith(".csv"):
                    match_date = re.search(r'\d{8}', nom_f)
                    if match_date:
                        date_veille_str = match_date.group()
                        nom_archive = f"{PREFIXE_LOG}{date_veille_str}.csv"
                        chemin_archive = os.path.join(REPERTOIRE_HISTO_REEL, nom_archive)
                        if not os.path.exists(chemin_archive) and os.path.getsize(chemin_complet) > 150:
                            import shutil
                            shutil.copy2(chemin_complet, chemin_archive)
                            print(f"\n[ARCHIVAGE VEILLE] {nom_f} -> {nom_archive}")

                try:
                    os.remove(chemin_complet)
                    print(f"[NETTOYAGE TEMPORAIRE] Fichier de la veille supprimé : {nom_f}")
                except Exception as e_del:
                    print(f"Impossible de supprimer {nom_f} : {e_del}")
    except Exception as e_net:
        print(f"Alerte nettoyage des fichiers de la veille : {e_net}")


def executer_purge_et_reinitialisation_seance_a_chaud(date_instant_pc):
    import os as system_os
    global volumes_seance, capitaux_seance, cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, historique_phase_precedente, registre_transactions_RAM, registre_RAM_t_moins_1, df_master_ram_cote, EST_ARCHIVE_FINALE_ENGAGEE, DATE_SEANCE_EN_COURS, REPERTOIRE_JSON, FICHIER_HISTO_RAMDISK, FICHIER_ETAT_RAM, vague_en_cours_prix, vague_en_cours_sens
    
    print(f"\n[QUARTZ] Alignement du moteur sur la séance du {date_instant_pc}...")
    
    volumes_seance.clear()
    capitaux_seance.clear()
    cumul_qte_achat.clear()
    cumul_vol_achat.clear()
    cumul_qte_vente.clear()
    cumul_vol_vente.clear()
    cumul_qte_indecis.clear()
    cumul_vol_indecis.clear()
    historique_volumes_qte.clear()
    historique_volumes_montant.clear()
    historique_dernier_cours.clear()
    historique_phase_precedente.clear()
    registre_transactions_RAM.clear()
    registre_RAM_t_moins_1.clear()
    vague_en_cours_prix.clear()
    vague_en_cours_sens.clear()
    
    df_master_ram_cote = None
    EST_ARCHIVE_FINALE_ENGAGEE = False
    DATE_SEANCE_EN_COURS = date_instant_pc
    
    verifier_et_nettoyer_fichiers_veille(date_instant_pc)


# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Registre des Compartiments de Négociation
# # PARTIE 5 / 20 : REGISTRE D AUTORITE DES GROUPES ET LIQUIDITES DE PLACE
# ==============================================================================
DICT_VALEURS_BVMT = {
    "MONOPRIX": {"groupe": 11}, "SFBT": {"groupe": 11}, "TUNISAIR": {"groupe": 11},
    "TUNISIE LEASING F": {"groupe": 11}, "BT": {"groupe": 11}, "STB": {"groupe": 11},
    "BNA": {"groupe": 11}, "AMEN BANK": {"groupe": 11}, "ATB": {"groupe": 11},
    "UIB": {"groupe": 11}, "SIMPAR": {"groupe": 12}, "CIL": {"groupe": 11},
    "ATL": {"groupe": 11}, "POULINA GP HOLDING": {"groupe": 11}, "SOTETEL": {"groupe": 11},
    "SOTUVER": {"groupe": 11}, "SOTUMAG": {"groupe": 11}, "SIAME": {"groupe": 11},
    "ATTIJARI LEASING": {"groupe": 11}, "SOTRAPIL": {"groupe": 11}, "SOMOCER": {"groupe": 11},
    "ASSAD": {"groupe": 11}, "ESSOUKNA": {"groupe": 11}, "TPR": {"groupe": 11},
    "ARTES": {"groupe": 11}, "TUNIS RE": {"groupe": 11}, "CARTHAGE CEMENT": {"groupe": 11},
    "ENNAKL AUTOMOBILES": {"groupe": 11}, "LAND OR": {"groupe": 11}, "ONE TECH HOLDING": {"groupe": 11},
    "NEW BODY LINE": {"groupe": 11}, "CITY CARS": {"groupe": 11}, "EURO-CYCLES": {"groupe": 11},
    "CELLCOM": {"groupe": 11}, "SAH": {"groupe": 11}, "MPBS": {"groupe": 11},
    "SOTIPAPIER": {"groupe": 11}, "TAWASOL GP HOLDING": {"groupe": 11}, "DELICE HOLDING": {"groupe": 11},
    "UADH": {"groupe": 12}, "UNIMED": {"groupe": 11}, "ATELIER MEUBLE INT": {"groupe": 11},
    "BTE (ADP)": {"groupe": 12}, "SPDIT - SICAF": {"groupe": 11}, "AIR LIQUIDE TSIE": {"groupe": 12},
    "PLAC. TSIE-SICAF": {"groupe": 12}, "ASTREE": {"groupe": 12}, "ALKIMIA": {"groupe": 12},
    "TUNINVEST-SICAR": {"groupe": 12}, "STAR": {"groupe": 11}, "MAGASIN GENERAL": {"groupe": 11},
    "BH ASSURANCE": {"groupe": 12}, "SIPHAT": {"groupe": 12}, "BH LEASING": {"groupe": 12},
    "SITS": {"groupe": 12}, "WIFACK INT BANK": {"groupe": 11}, "CIMENTS DE BIZERTE": {"groupe": 12},
    "BEST LEASE": {"groupe": 12}, "SOTEMAIL": {"groupe": 12}, "OFFICEPLAST": {"groupe": 11},
    "SANIMED": {"groupe": 12}, "SITEX": {"groupe": 12}, "BNA ASSURANCES": {"groupe": 11},
    "STE TUN. DU SUCRE": {"groupe": 12}, "STIP": {"groupe": 12}, "HANNIBAL LEASE": {"groupe": 12},
    "ATTIJARI BANK": {"groupe": 11}, "TELNET HOLDING": {"groupe": 11}, "BH BANK": {"groupe": 11},
    "ICF": {"groupe": 11}, "AETECH": {"groupe": 12}, "UBCI": {"groupe": 11},
    "ASSUR MAGHREBIA": {"groupe": 11}, "BIAT": {"groupe": 11}, "SMART TUNISIE": {"groupe": 11},
    "STA": {"groupe": 11}, "ASSU MAGHREBIA VIE": {"groupe": 11}
}
# Extraction unifiée matricielle
LISTE_VALEURS_UNIFIEES = list(DICT_VALEURS_BVMT.keys())

# Allocation des registres de vagues en RAM
vague_en_cours_prix = {ticker: 0.0 for ticker in LISTE_VALEURS_UNIFIEES}
vague_en_cours_sens = {ticker: "NEUTRE" for ticker in LISTE_VALEURS_UNIFIEES}

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Allocation Mémoires en Structure RAMDISK
# # PARTIE 6 / 20 : ALLOCATION ET INITIALISATION GENERALE DES COMPTEURS UNITAIRES
# ==============================================================================

# Dictionnaires unitaires d activité transactionnelle de session
volumes_seance = {ticker: 0 for ticker in LISTE_VALEURS_UNIFIEES}
capitaux_seance = {ticker: 0.0 for ticker in LISTE_VALEURS_UNIFIEES}

# Registres de mémorisation glissante pour séries de ticks unitaires
historique_courbe_ticks = {ticker: [] for ticker in LISTE_VALEURS_UNIFIEES}
derniere_nature_valide_titre = {ticker: "NEUTRE" for ticker in LISTE_VALEURS_UNIFIEES}

# Registres de couplage asynchrone pour la mémoire glissante t-1
registre_RAM_t_moins_1 = {}
registre_transactions_RAM = []
derniere_action_traitee = {}

# Calage initial à froid de l horloge centrale maîtresse
HEURE_CENTRALE_BVMT = "08:30:00"

for action in LISTE_VALEURS_UNIFIEES:
    cumul_qte_achat[action] = 0
    cumul_vol_achat[action] = 0.0
    cumul_qte_vente[action] = 0
    cumul_vol_vente[action] = 0.0
    cumul_qte_indecis[action] = 0
    cumul_vol_indecis[action] = 0.0
    
    historique_volumes_qte[action] = 0
    historique_volumes_montant[action] = 0.0
    historique_dernier_cours[action] = 0.0
    
    # Structure de résilience : Initialisation du dictionnaire t-1 d amorti anti-KeyError
    registre_RAM_t_moins_1[action] = {col: "0" for col in LISTE_ENTETES_OFFICIELS}
    registre_RAM_t_moins_1[action]["SbeMarket SheetValeur"] = action
    registre_RAM_t_moins_1[action]["Etat_Num"] = 5
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Gestionnaire du Calendrier Glissant de Place
# # PARTIE 7 / 20 : RECONNAISSANCE DU REGIME HORAIRE (HIVER / ETE / RAMADAN)
# ==============================================================================
# Table de projection de l axe lunaire validée par la Bourse de Tunis
TABLE_RAMADAN_BVMT = {
    2024: (3, 11), 2025: (3, 1), 2026: (2, 18), 2027: (2, 7), 2028: (1, 27),
    2029: (1, 16), 2030: (1, 5), 2031: (12, 25), 2032: (12, 14), 2033: (12, 3)
}

def detecter_regime_horaire(date_obj):
    """
    Identifie le régime horaire de la Place de Tunis pour adapter les états de séance.
    0% Risque d'erreur de chevauchement sur les grilles de pré-clôture juillet/août.
    """
    try:
        annee = date_obj.year
        
        # 🌙 Interception de la fenêtre mobile du Ramadan
        if annee in TABLE_RAMADAN_BVMT:
            mois_r, jour_r = TABLE_RAMADAN_BVMT[annee]
            date_pivot_ramadan = datetime.date(annee, mois_r, jour_r)
            debut_ramadan = date_pivot_ramadan - datetime.timedelta(days=2)
            fin_ramadan = debut_ramadan + datetime.timedelta(days=35)
            
            date_pure_test = date_obj if isinstance(date_obj, datetime.date) else date_obj.date()
            if debut_ramadan <= date_pure_test <= fin_ramadan: 
                return "ETE_RAMADAN"
                
        # ☀️ Interception réglementaire de la Séance Unique d'Été (Juillet, Août)
        if date_obj.month in[7, 8]: 
            return "ETE_RAMADAN"
            
    except Exception as e_cal: 
        print(f"⚠️ Alerte décodeur calendrier boursier : {e_cal}")
        
    return "HIVER"
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Outils Microstructurels de Typage de Données
# # PARTIE 8 / 20 : CONVERTISSEUR COMPTABLE POINT FLOTTANT ET PROTECTION SÉMANTIQUE
# ==============================================================================
def convert_num(val_str):
    """
    Nettoie et convertit les trames brutes textuelles de Place en réels double précision.
    Annihile les crashs liés aux chaînes de caractères descriptives et aux espaces insécables.
    """
    try:
        if pd.isna(val_str) or val_str is None: 
            return 0.0
            
        v = str(val_str).strip().upper()
        if "ATP" in v: return -1.0
        if "OUV" in v: return -2.0
        
        # Coupe-circuit sémantique pour interdire le traitement arithmétique des phases
        if any(keyword in v for keyword in ["CONTINU", "RESERV", "GEL", "FIXING", "SUSPENDU", "ATTENTE", "MARCHE", "FERME", "VEILLE", "CLOTURE"]):
            return 0.0
            
        # Élimination des espaces insécables de Place (\xa0, \u202f) et conversion du séparateur decimal
        v_nettoye = re.sub(r'[\s\xa0\u202f]', '', v).replace(',', '.')
        
        match_float = re.search(r"[-+]?\d*\.\d+|\d+", v_nettoye)
        if match_float:
            return float(match_float.group())
        return 0.0
    except:
        return 0.0

def _forcer_float_reprise(valeur_cellule):
    """
    Force l extraction scalaire d une cellule lors de la synchronisation à froid.
    """
    if pd.isna(valeur_cellule) or str(valeur_cellule).strip() == "": 
        return 0.0
    try: 
        str_nettoye = str(valeur_cellule).replace(" ", "").replace(",", ".").strip()
        return float(str_nettoye)
    except: 
        return 0.0
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Machine d'État de Réservation Volatilité
# # PARTIE 9 / 20 : EN-TÊTE DU COMPARTIMENT DE GEL ET PALIERS REGLEMENTAIRES
# ==============================================================================
# Registre dynamique scellé à 7 clés par instrument pour piloter les fixing de reprise
registre_reservation_groupe11 = {
    t: {
        "en_gel": False, 
        "etape": 0, 
        "debut_gel_dt": None, 
        "fin_gel_dt": None,
        "pivot_dynamique": 0.0, 
        "seuil_actuel_pourcent": 3.0, 
        "ordres_accumules": []
    } for t in LISTE_VALEURS_UNIFIEES
}
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Gestionnaire Chronométrique Atos NSC
# # PARTIE 10 / 20 : VÉRIFICATEUR DES PHASES PAR COMPARTIMENT ET RÉGIME
# ==============================================================================
def verifier_phase_par_groupe(heure_api_str, groupe_id, regime, action=None):
    """
    Détermine la phase théorique du marché selon le groupe de cotation et le calendrier.
    🎯 SCELLAGE DE L'ÉTAT 0 : À partir de 15h00, coupe-circuit nocturne strict engagé.
    """
    if datetime.datetime.now().weekday() >= 5 and MODE_PRODUCTION:
        return 0, "WEEK-END Marche ferme (Samedi/Dimanche)", "00:00:00"
    
    
    h = str(heure_api_str).strip().replace(":", "")
    if len(h) == 6: 
        h_f = f"{h[:2]}:{h[2:4]}:{h[4:6]}"
    else: 
        h_f = datetime.datetime.now().strftime("%H:%M:%S")

    # 🛑 Clôture absolue de la session comptable à 15h00
    if h_f >= "15:00:00":
        return 0, "CLOTURE Marche ferme (Extinction Falcon)", "00:00:00"

    # Phase de nettoyage de nuit
    if "08:00:00" <= h_f < "08:30:00": 
        return 1, "Cloture de veille / Nettoyage carnet (Purge)", "08:30:00"
    
    # Awakening / Saisie des ordres sans exécution
    if h_f == "08:30:00" or ("08:30:00" <= h_f < "08:30:05"):
        return 2, "Preouverture active / Saisie des ordres", "08:30:05"
    
    # Pre-Fixing d'ouverture nominal
    if "08:30:05" <= h_f < "09:00:00": 
        return 3, "Pre_Fixing d'ouverture (Accumulation)", "09:00:00"

    # ==========================================================================
    # 📯 CASE A : GRILLE DES HORAIRES DU RÉGIME D'HIVER STANDARD
    # ==========================================================================
    if regime == "HIVER":
        # Valeurs traitées en Continu (Groupes 11, 21, 41, 61, 71, 81)
        if groupe_id in (11, 21, 41, 61, 71, 81):
            if h_f == "09:00:00": return 4, "Fixing d'ouverture", "09:00:01"
            elif "09:00:01" <= h_f < "14:00:00": return 5, "Seance ordinaire (Negociation en continue)", "14:00:00"
            elif "14:00:00" <= h_f < "14:05:00": return 3, "Pre_Fixing de cloture", "14:05:00"
            elif h_f == "14:05:00": return 4, "Second Fixing de cloture", "14:05:01"
            elif "14:05:01" <= h_f < "14:15:00": return 5, "Negociation au dernier cours de cloture", "14:15:00"
            elif "14:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"

        # Valeurs traitées en Double Fixing (Groupes 12, 22, 42)
        elif groupe_id in (12, 22, 42):
            if h_f == "09:00:00": return 4, "Ouverture / Fixing (Fix 1)", "09:00:01"
            elif "09:00:01" <= h_f < "09:15:00": return 5, "Negociation au Dernier Cours (Fix 1)", "09:15:00"
            elif "09:15:00" <= h_f < "14:00:00": return 3, "Pre_Fixing (Attente 2nd Fixing)", "14:00:00"
            elif h_f == "14:00:00": return 4, "Cloture / Second Fixing (Fix 2)", "14:00:01"
            elif "14:00:01" <= h_f < "14:15:00": return 5, "Negociation au Dernier Cours (Fix 2)", "14:15:00"
            elif "14:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
                
        # Valeurs traitées en Fixing Unique (Groupes 13, 52, 95, 99)
        elif groupe_id in (13, 52, 95, 99):
            if "08:30:01" <= h_f < "12:00:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "12:00:00"
            elif h_f == "12:00:00": return 4, "Fixing Unique (12h00)", "12:00:01"
            elif "12:00:01" <= h_f < "12:15:00": return 5, "Negociation au cours de fixing", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"
                
        # Compartiment Droits d'Attributions (Groupe 32)
        elif groupe_id == 32:
            if "08:30:01" <= h_f < "13:45:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "13:45:00"
            elif h_f == "13:45:00": return 4, "Fixing Unique (13h45)", "13:45:01"
            elif "13:45:01" <= h_f < "14:15:00": return 5, "Negociation au cours de fixing", "14:15:00"
            elif "14:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
                
        # Compartiment Hors-Cote Libre (Groupe 33)
        elif groupe_id == 33:
            if "08:30:01" <= h_f < "12:15:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "12:15:00"
            elif h_f == "12:15:00": return 4, "Fixing Unique (12h15)", "12:15:01"
            elif "12:15:01" <= h_f < "12:45:00": return 5, "Negociation au cours de fixing (Hors-Cote)", "12:45:00"
            elif "12:45:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Temporisation du Régime Horaire Allégé
# # PARTIE 11 / 20 : HORAIRES DU LOGICIEL POUR SÉANCE UNIQUE / RAMADAN
# ==============================================================================
    else:
        # Grille Horaire d Été / Ramadan (Groupes Liquides Continu)
        if groupe_id in (11, 21, 41, 61, 71, 81):
            if h_f == "09:00:00": return 4, "Fixing d'ouverture", "09:00:01"
            elif "09:00:01" <= h_f < "12:00:00": return 5, "Seance ordinaire (Negociation en continue)", "12:00:00"
            elif "12:00:00" <= h_f < "12:05:00": return 3, "Pre_Fixing de cloture", "12:05:00"
            elif h_f == "12:05:00": return 4, "Second Fixing de cloture", "12:05:01"
            elif "12:05:01" <= h_f < "12:15:00": return 5, "Negociation au dernier cours de cloture", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
            
        # Groupes Double Fixing en période d Été
        elif groupe_id in (12, 22, 42):
            if h_f == "09:00:00": return 4, "Ouverture / Fixing (Fix 1)", "09:00:01"
            elif "09:00:01" <= h_f < "09:15:00": return 5, "Negociation au Dernier Cours (Fix 1)", "09:15:00"
            elif "09:15:00" <= h_f < "12:00:00": return 3, "Pre_Fixing (Attente 2nd Fixing)", "12:00:00"
            elif h_f == "12:00:00": return 4, "Cloture / Second Fixing (Fix 2)", "12:00:01"
            elif "12:00:01" <= h_f < "12:15:00": return 5, "Negociation au Dernier Cours (Fix 2)", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
            
        # Compartiments Fixing Unique en Été
        elif groupe_id in (13, 52, 95, 99):
            if "08:30:01" <= h_f < "10:00:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "10:00:00"
            elif h_f == "10:00:00": return 4, "Fixing Unique (10h00)", "10:00:01"
            elif "10:00:01" <= h_f < "10:15:00": return 5, "Negociation au cours de fixing", "10:15:00"
            elif "10:15:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"
            
        # Droits d Attribution en Été
        elif groupe_id == 32:
            if "08:30:01" <= h_f < "11:45:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "11:45:00"
            elif h_f == "11:45:00": return 4, "Fixing Unique (11h45)", "11:45:01"
            elif "11:45:01" <= h_f < "12:15:00": return 5, "Negociation au cours de fixing", "12:15:00"
            elif "12:15:00" <= h_f < "15:00:00": return 6, "Marche des Blocs (Declaration)", "15:00:00"
            
        # Hors-Cote Libre en Été
        elif groupe_id == 33:
            if "08:30:01" <= h_f < "10:15:00": return 3, "Pre_Fixing (Accumulation d'ordres)", "10:15:00"
            elif h_f == "10:15:00": return 4, "Fixing Unique (10h15)", "10:15:01"
            elif "10:15:01" <= h_f < "10:45:00": return 5, "Negociation au cours de fixing (Hors-Cote)", "10:45:00"
            elif "10:45:00" <= h_f < "15:00:00": return 1, "Marche en attente de cloture", "15:00:00"

    # Injection du verrou de l état de Réservation Volatilité officiel de Place (État 9)
    if groupe_id in (11, 51) and "action" in locals() and registre_reservation_groupe11[action]["en_gel"]: 
        return 9, "Reservation Volatilit", "00:00:00"

    return 1, "Marche en attente", "00:00:00"
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Surveillance HFT des Écarts Nets de Place
# # PARTIE 12 / 20 : CALCULATEUR DE CRASH, DISPOSITIF STATIQUE ET DYNAMIQUE VELOCITÉ
# ==============================================================================
def calculer_alerte_seuil_bvmt(dernier_cours, cours_reference_veille, groupe_id, action_name, heure_machine_dt, meilleur_achat=0.0, meilleure_vente=0.0):
    global registre_reservation_groupe11, historique_volumes_qte
    
    if cours_reference_veille <= 0 or dernier_cours <= 0: 
        return "Normal", dernier_cours
        
    info_gel = registre_reservation_groupe11[action_name]
    variation_veille = ((dernier_cours - cours_reference_veille) / cours_reference_veille) * 100.0
    abs_var_veille = abs(variation_veille)
    
    qte_courante_seance = globals().get("qte_totale_seance_current", 0)
    a_eu_une_execution_reelle = False
    if action_name in historique_volumes_qte and qte_courante_seance > historique_volumes_qte[action_name]:
        a_eu_une_execution_reelle = True
        
    if groupe_id in (11, 12, 51, 52, 99):
        # 🔒 PLAFOND STATIQUE SCELLÉ IMMUABLE : Coupe-circuit ultime du carnet NSC
        if abs_var_veille >= 6.09 and a_eu_une_execution_reelle:
            return f"CRITIQUE : Plafond Informatique Absolu Atteint ({variation_veille:+.2f}%) - Verrou NSC", dernier_cours
            
        if groupe_id in (11, 51):
            if info_gel["pivot_dynamique"] == 0.0:
                info_gel["pivot_dynamique"] = dernier_cours

            if not info_gel["en_gel"]:
                # 🎯 INTERCEPTION PRÉDICTIVE SÉMANTIQUE :
                # Analyse les meilleures limites du carnet pour lever l alerte imminente avant l exécution
                for prix_test in [dernier_cours, meilleur_achat, meilleure_vente]:
                    if prix_test is None or prix_test <= 0: continue
                    var_dynamique_theo = ((prix_test - info_gel["pivot_dynamique"]) / info_gel["pivot_dynamique"]) * 100.0
                    
                    if abs(var_dynamique_theo) >= 3.00:
                        return f"ALERTE : Réservation Imminente Détectée ({var_dynamique_theo:+.2f}%)", info_gel["pivot_dynamique"]
                
                info_gel["pivot_dynamique"] = dernier_cours
                return "Normal", dernier_cours
            else:
                # 🗄️ MACHINE D ÉTAT DES PALIERS DE FIXING DE REPRISE SBE FALCON
                if a_eu_une_execution_reelle or len(info_gel["ordres_accumules"]) == 0:
                    if len(info_gel["ordres_accumules"]) == 0 or dernier_cours != info_gel["ordres_accumules"][-1]:
                        info_gel["ordres_accumules"].append(dernier_cours)
                
                if heure_machine_dt >= info_gel["fin_gel_dt"]:
                    if len(info_gel["ordres_accumules"]) > 0:
                        prix_equilibre_theorique = round(sum(info_gel["ordres_accumules"]) / len(info_gel["ordres_accumules"]), 3)
                    else:
                        prix_equilibre_theorique = dernier_cours
                        
                    var_equilibre = ((prix_equilibre_theorique - info_gel["pivot_dynamique"]) / info_gel["pivot_dynamique"]) * 100.0
                    
                    if abs(var_equilibre) < info_gel["seuil_actuel_pourcent"]:
                        info_gel["en_gel"] = False
                        info_gel["ordres_accumules"] = [] 
                        ancien_pivot = info_gel["pivot_dynamique"]
                        info_gel["pivot_dynamique"] = prix_equilibre_theorique
                        return f"REPRISE CONTINU : Fixing réussi à {prix_equilibre_theorique:.3f} TND", ancien_pivot
                    else:
                        # Escalades successives par paliers réglementaires
                        if info_gel["etape"] == 1:
                            info_gel["etape"] = 2
                            info_gel["seuil_actuel_pourcent"] = 6.09
                            info_gel["fin_gel_dt"] = heure_machine_dt + datetime.timedelta(minutes=10)
                            return "ESCALADE ÉTAPE 2 : Seuil élargi à 4.50% [10 Min]", info_gel["pivot_dynamique"]
                        elif info_gel["etape"] == 2:
                            info_gel["etape"] = 3
                            info_gel["seuil_actuel_pourcent"] = 6.09
                            info_gel["fin_gel_dt"] = heure_machine_dt + datetime.timedelta(minutes=5)
                            return "ESCALADE ÉTAPE 3 : Entrée Dernier Palier 6.09% [5 Min]", info_gel["pivot_dynamique"]
                        else:
                            info_gel["en_gel"] = False
                            info_gel["ordres_accumules"] = [] 
                            return "FORÇAGE ÉTAPE SUCCESSIVE", info_gel["pivot_dynamique"]
                else:
                    return f"GEL ACTIF : Fin à {info_gel['fin_gel_dt'].strftime('%H:%M:%S')}", info_gel["pivot_dynamique"]
        else:
            var_fixing_theo = ((dernier_cours - cours_reference_veille) / cours_reference_veille) * 100.0
            if abs(var_fixing_theo) >= 3.00 and a_eu_une_execution_reelle:
                return f"ALERTE : Tri de Réservation Fixing (Veille {var_fixing_theo:+.2f}%)", cours_reference_veille
            return "Normal", cours_reference_veille
            
    elif groupe_id == 32:
        if abs_var_veille >= 250.00 and a_eu_une_execution_reelle: return "CRITIQUE : Plafond Droits (±250%)", cours_reference_veille
        elif abs_var_veille >= 15.00 and a_eu_une_execution_reelle: return "ALERTE : Seuil Droits (±15%)", cours_reference_veille
    elif groupe_id in (21, 41) and abs_var_veille >= 10.00 and a_eu_une_execution_reelle:
        return "CRITIQUE : Seuil Obligataire Unique (10%)", cours_reference_veille        
    return "Normal", cours_reference_veille
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> Analyse de l Agressivité des Flux Réseau SBE
# # PARTIE 13 / 20 : ALGORITHME DE SURVEILLANCE ET DÉPISTAGE DES ORDRES EN DIRECT
# ==============================================================================
def exécuter_moteur_lee_ready(action, delta_qte, delta_vol, dernier_c, id_phase_t0, phase_txt_t0, p_achat_t0, p_vente_t0, qte_totale_seance, vol_argent_seance, alerte_seuil_active, imbalance_carnet_t0, spread_pourcent_t0, variation_seance, cours_reference_seance, row, a_trouve_un_trade, heure_ticker, courbe_txt):
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, historique_courbe_ticks, registre_transactions_RAM, derniere_action_traitee, derniere_nature_valide_titre, registre_RAM_t_moins_1, vague_en_cours_prix, vague_en_cours_sens
    
    cmp_officiel_marche = convert_num(row.get("CMP", dernier_c))
    if cmp_officiel_marche <= 0: 
        cmp_officiel_marche = dernier_c

    # Calcul du prix d exécution pondéré réel de l opération (Delta Vol / Delta Qte)
    if delta_qte > 0 and delta_vol > 0:
        prix_execution_reel = round(delta_vol / delta_qte, 3)
    else:
        prix_execution_reel = dernier_c

    nature_ms = "NEUTRE"
    t1 = registre_RAM_t_moins_1.get(action, {})
    ancien_sens = derniere_nature_valide_titre.get(action, "NEUTRE")
    
    if t1 and "Dernier" in t1:
        p_achat_t1_str = str(t1.get("Achat", "0")).strip().upper()
        p_vente_t1_str = str(t1.get("Vente", "0")).strip().upper()
        p_achat_t1 = convert_num(p_achat_t1_str)
        p_vente_t1 = convert_num(p_vente_t1_str)
        dernier_t1 = convert_num(t1.get("Dernier", 0.0))
        id_phase_t1 = int(t1.get("Etat_Num", 5))
        
        # Noyau Continu d Interception Lee-Ready (Phases de négociation active)
        if id_phase_t0 in (4, 5, 7) and id_phase_t1 in (4, 5, 7):
            # Détection des flux acheteurs agressifs multiniveaux (Franchissement de la Vente t-1)
            if p_vente_t1 > 0 and prix_execution_reel > p_vente_t1:
                nature_ms = "ACHAT"
                courbe_txt = "▲ ACHAT ATP multiniveau détecté"
            # Détection des flux vendeurs agressifs multiniveaux (Enfoncement de l Achat t-1)
            elif p_achat_t1 > 0 and prix_execution_reel < p_achat_t1:
                nature_ms = "VENTE"
                courbe_txt = "▼ VENTE ATP multiniveau détecté"
            else:
                if "ATP" in p_vente_t1_str or "OUV" in p_vente_t1_str or p_vente_t1 <= 0:
                    nature_ms = "ACHAT" if prix_execution_reel >= dernier_t1 else "VENTE"
                elif "ATP" in p_achat_t1_str or "OUV" in p_achat_t1_str or p_achat_t1 <= 0:
                    nature_ms = "VENTE" if prix_execution_reel <= dernier_t1 else "ACHAT"
                else:
                    if prix_execution_reel >= p_vente_t1: nature_ms = "ACHAT"
                    elif prix_execution_reel <= p_achat_t1: nature_ms = "VENTE"
                    else:
                        # Règle d inertie tick intérieur
                        if prix_execution_reel == dernier_t1 and ancien_sens in ["ACHAT", "VENTE"]:
                            nature_ms = ancien_sens
                        else:
                            nature_ms = "ACHAT" if prix_execution_reel > dernier_t1 else ("VENTE" if prix_execution_reel < dernier_t1 else "NEUTRE")
        # Ingestion Tick de Clôture / Fixing
        else:
            if prix_execution_reel == dernier_t1:
                if ancien_sens in ["ACHAT", "VENTE"]: nature_ms = ancien_sens  
            else:
                nature_ms = "ACHAT" if prix_execution_reel > dernier_t1 else "VENTE"
            
            if nature_ms == "NEUTRE" and id_phase_t0 in (2, 3):
                if cours_reference_seance > 0 and prix_execution_reel != cours_reference_seance:
                    nature_ms = "ACHAT" if prix_execution_reel > cours_reference_seance else "VENTE"
    else:
        if variation_seance > 0: nature_ms = "ACHAT"
        elif variation_seance < 0: nature_ms = "VENTE"
        else: nature_ms = "NEUTRE"

    if nature_ms == "NEUTRE" and ancien_sens in ["ACHAT", "VENTE"]:
        nature_ms = ancien_sens

    if nature_ms in ["ACHAT", "VENTE"]:
        derniere_nature_valide_titre[action] = nature_ms

    dernier_c = prix_execution_reel
    
    # 🔍 ALGORITHME SCELLÉ DE DÉTECTION ET QUALIFICATION DU STATUT DE LA VAGUE
    dernier_prix_vague = vague_en_cours_prix.get(action, 0.0)
    dernier_sens_vague = vague_en_cours_sens.get(action, "NEUTRE")
    
    if round(float(dernier_c), 3) != round(float(dernier_prix_vague), 3) or nature_ms != dernier_sens_vague:
        statut_vague = "DEBUT"
        vague_en_cours_prix[action] = dernier_c
        vague_en_cours_sens[action] = nature_ms
    else:
        statut_vague = "CONTINUATION"
    
    # Gravure de la jauge d orientation de Place (Tableau de 6 tics)
    courbe_loc = historique_courbe_ticks.get(action, [])
    char_tendance = "S" if nature_ms == "NEUTRE" else ("H" if nature_ms == "ACHAT" else "B")
    courbe_loc.append(char_tendance)
    if len(courbe_loc) > 6: courbe_loc.pop(0)
    jauge_txt = "".join(courbe_loc).ljust(6, "_")
    
    if "multiniveau" not in courbe_txt:
        courbe_txt = f"{char_tendance} {nature_ms.ljust(5)} [{jauge_txt}]"

    # Alimentation sCELLée des 42 structures de données de la ligne transactionnelle
    log_operation_geant = {
        "Ticker": action, "Heure_Marche": heure_ticker, "Heure_Python": time.strftime('%H:%M:%S'),          
        "Nature_Operation": nature_ms, "Delta_Qte": int(delta_qte), "Prix_Execution": round(float(dernier_c), 3), "Delta_Vol": round(float(delta_vol), 3),             
        "T_Phase_Reglementaire": str(phase_txt_t0), "T_Alerte_Volatilis": statut_vague, "T_Alerte_Seuil": str(alerte_seuil_active), "T_Graph_Flash": str(jauge_txt), 
        "T0_CMP": round(float(cmp_officiel_marche), 3), "T0_Etat": str(phase_txt_t0), "T0_Qt_A": str(row.get("Qt. A", "0")), "T0_Achat": str(p_achat_t0), 
        "T0_Vente": str(p_vente_t0), "T0_Qt_V": str(row.get("Qt.V", "0")), "T0_Cloture": str(row.get("Clôture", "0")), "T0_CTO": str(row.get("CTO", "0")), 
        "T0_VTO": str(row.get("VTO", "0")), "T0_Ouverture": str(row.get("Ouverture", "0")), "T0_Dernier": str(row.get("Dernier", "0")),
        "T0_Var": str(row.get("Var.", "0")), "T0_CMP_Brut": f"{cmp_officiel_marche:.3f}", "T0_Qte_Seance": int(qte_totale_seance), "T0_Volume_Seance": round(float(vol_argent_seance), 3), "T0_Heure_Marche": heure_ticker,
        # 🟢 RESTAURATION DES LIAISONS GLISSANTES T-1 (TYPE SBE ATOS NOMINAL)
        "T1_Etat": str(t1.get("Etat", "5")), 
        "T1_Qt_A": str(t1.get("Qt. A", "0")), 
        "T1_Achat": str(t1.get("Achat", "0")), 
        "T1_Vente": str(t1.get("Vente", "0")), 
        "T1_Qt_V": str(t1.get("Qt.V", "0")), 
        "T1_Cloture": str(t1.get("Clôture", "0")), 
        "T1_CTO": str(t1.get("CTO", "0")), 
        "T1_VTO": str(t1.get("VTO", "0")), 
        "T1_Ouverture": str(t1.get("Ouverture", "0")), 
        "T1_Dernier": str(t1.get("Dernier", "0")), 
        "T1_Var": str(t1.get("Var.", "0")), 
        "T1_CMP": str(t1.get("CMP", "0")), 
        "T1_Qte_Seance": int(convert_num(t1.get("Qté", 0))), 
        "T1_Volume_Seance": round(float(convert_num(t1.get("Volume", 0.0))), 3)
    }
    registre_transactions_RAM.append(log_operation_geant)
    derniere_action_traitee = log_operation_geant.copy()
    
    a_trouve_un_trade = True
    return a_trouve_un_trade, courbe_txt, nature_ms


# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : INTÉGRATEUR VECTORIEL MICROSTRUCTUREL SBE
# # Fichier : bourse_macsa.py -> def analyser_boucle_microstructure() (PARTIE 1 DE 3)
# # SCellage : Ingestion dictionnaire RAM ultra-rapide et initialisation des listes
# ==============================================================================
def analyser_boucle_microstructure(df_final_cote, regime_actif, debut_calcul):
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, historique_phase_precedente, registre_transactions_RAM, derniere_action_traitee, volumes_seance, capitaux_seance, registre_RAM_t_moins_1, HEURE_CENTRALE_BVMT, FICHIER_HISTO_RAMDISK, MODE_PRODUCTION
    try:
        liste_phases, listes_microstructure, liste_volatiles = [], [], []
        l_qte_a, l_vol_a, l_qte_v, l_vol_v, l_qte_i, l_vol_i = [], [], [], [], [], []
        l_spread_p, l_imbalance_c = [], []
        l_etat_brut_api, l_etat_calcule_systeme = [], []
        l_heure_brute_api, l_heure_calculee_pc = [], []
        l_achat, l_vente, l_dernier, l_cloture, l_cto, l_vto, l_ouverture = [], [], [], [], [], [], []
        l_cmp, l_qte_seance, l_vol_seance, l_p_haut, l_p_bas = [], [], [], [], []
        l_sh, l_sb, l_dh, l_db = [], [], [], []
        l_variation = []
        a_trouve_un_trade, courbe_txt = False, "■ STABLE  [______]"
        
        maintenant_dt = datetime.datetime.now()
        heure_machine_formatee = maintenant_dt.strftime("%H:%M:%S")
        heure_marche_formatee = HEURE_CENTRALE_BVMT
        heure_api_verifiee = heure_marche_formatee
        heure_marche_active_titre = heure_marche_formatee

        cle_variation_api = "Var."
        for col_name in df_final_cote.columns:
            if "VAR" in str(col_name).upper(): 
                cle_variation_api = col_name
                break
                
        # Indexation par dictionnaire RAM ultra-rapide (0% latence CPU)
        dict_api_indexe = df_final_cote.to_dict(orient='index')

        for action in LISTE_VALEURS_UNIFIEES:
            action_cle = str(action).strip().upper()
            
            row = dict_api_indexe.get(action)
            if row is None:
                row = dict_api_indexe.get(action_cle)
                if row is None: 
                    continue
                
            groupe_attribue = DICT_VALEURS_BVMT[action]["groupe"]
            
            cours_reference_seance = convert_num(row.get("Clôture", 0.0))
            if cours_reference_seance <= 0: 
                cours_reference_seance = convert_num(row.get("CTO", 0.0))
            
            qte_brute_api = convert_num(row.get("Qté", 0))
            vol_brut_api = convert_num(row.get("Volume", 0.0))
            dernier_brut_api = convert_num(row.get("Dernier", 0.0))
            
            if qte_brute_api <= 0 and action in historique_volumes_qte and historique_volumes_qte[action] > 0:
                qte_totale_seance = historique_volumes_qte[action]
                vol_argent_seance = historique_volumes_montant[action]
                dernier_c = historique_dernier_cours.get(action, cours_reference_seance)
            else:
                qte_totale_seance = qte_brute_api
                vol_argent_seance = vol_brut_api
                dernier_c = dernier_brut_api

            global qte_totale_seance_current
            qte_totale_seance_current = qte_totale_seance
            variation_seance = convert_num(str(row.get(cle_variation_api, "0.0")).replace("%", ""))
            
            p_achat_t0, p_vente_t0 = convert_num(row.get("Achat", 0.0)), convert_num(row.get("Vente", 0.0))
            q_achat_t0, q_vente_t0 = convert_num(row.get("Qt. A", 0)), convert_num(row.get("Qt.V", 0))

            heure_brute_titre = str(row.get("Heure", heure_api_verifiee)).strip()
            if len(heure_brute_titre) == 6 and heure_brute_titre.isdigit():
                heure_brute_titre = f"{heure_brute_titre[:2]}:{heure_brute_titre[2:4]}:{heure_brute_titre[4:6]}"
            
            l_heure_brute_api.append(heure_brute_titre)
            l_heure_calculee_pc.append(heure_machine_formatee)

            code_brut_api_texte = str(row.get("Etat", "5")).strip()
            l_etat_brut_api.append(code_brut_api_texte)
            # ==============================================================================
            # 🦅 STATION QUANTITATIVE MAC SA : COMMUTATEUR CHRONOMÉTRIQUE ET FILTRAGE TICK
            # # Fichier : bourse_macsa.py -> def analyser_boucle_microstructure() (PARTIE 2 DE 3)
            # # SCellage : Nettoyage syntaxique absolu (0% Instruction chaînée par ":")
            # ==============================================================================
            try:
                digit_brut = re.sub(r'[^\d]', '', code_brut_api_texte)
                state_num_physique_brut = int(digit_brut) if digit_brut != "" else int(row.get("Etat_Num", 5))
            except:
                state_num_physique_brut = 5

            state_reg_theorique, phase_reg, heure_cible_phase = verifier_phase_par_groupe(heure_marche_active_titre, groupe_attribue, regime_actif, action)
            
            alerte_seuil_active, pivot_retourne = calculer_alerte_seuil_bvmt(
                dernier_c, cours_reference_seance, groupe_attribue, action, maintenant_dt
            )

            if "12:00:00" <= heure_marche_active_titre < "12:05:00" and regime_actif != "HIVER" and groupe_attribue in (11, 21, 41):
                state_num_calcule = 3  
                phase_reg = "Pre_Fixing de cloture"
            elif "14:00:00" <= heure_marche_active_titre < "14:05:00" and regime_actif == "HIVER" and groupe_attribue in (11, 21, 41):
                state_num_calcule = 3  
                phase_reg = "Pre_Fixing de cloture"
            elif state_num_physique_brut == 8 or "SUSPENDU" in code_brut_api_texte.upper():
                state_num_calcule = 8
                phase_reg = "Cotation Suspendue"
            elif state_num_physique_brut == 9 or "RESERVE" in code_brut_api_texte.upper():
                state_num_calcule = 9
                phase_reg = "Reservation Volatilit"
                p_achat_t0 = round(pivot_retourne * 0.955, 3)
                p_vente_t0 = round(pivot_retourne * 1.045, 3)
            else:
                state_num_calcule = state_reg_theorique
                if state_num_calcule == 5: 
                    if "12:05" not in phase_reg and "14:05" not in phase_reg:
                        phase_reg = "Seance ordinaire (Negociation)"
                    else:
                        phase_reg = "Negociation au Dernier Cours"
                elif state_num_calcule == 3: 
                    phase_reg = "Pre_Fixing d'ouverture"

            l_etat_calcule_systeme.append(int(state_num_calcule))
            liste_phases.append(phase_reg)
            l_achat.append(p_achat_t0)
            l_vente.append(p_vente_t0)
            l_dernier.append(dernier_c)
            l_variation.append(variation_seance)
            l_cloture.append(cours_reference_seance)
            l_cto.append(convert_num(row.get("CTO", 0.0)))
            l_vto.append(convert_num(row.get("VTO", 0.0)))
            l_ouverture.append(convert_num(row.get("Ouverture", 0.0)))
            l_cmp.append(convert_num(row.get("CMP", dernier_c)))
            l_qte_seance.append(qte_totale_seance)
            l_vol_seance.append(vol_argent_seance)
            l_p_haut.append(convert_num(row.get("P. Haut", 0.0)))
            l_p_bas.append(convert_num(row.get("P. Bas", 0.0)))
            l_sh.append(convert_num(row.get("Seuil.S. Haut", 0.0)))
            l_sb.append(convert_num(row.get("Seuil.S. Bas", 0.0)))
            l_dh.append(convert_num(row.get("Seuil.D. Haut", 0.0)))
            l_db.append(convert_num(row.get("Seuil.D. Bas", 0.0)))

            spread_pourcent_t0 = (p_vente_t0 - p_achat_t0) / p_achat_t0 if p_achat_t0 > 0 else 0.0
            imbalance_carnet_t0 = (q_achat_t0 - q_vente_t0) / (q_achat_t0 + q_vente_t0) if (q_achat_t0 + q_vente_t0) > 0 else 0.0
            l_spread_p.append(spread_pourcent_t0)
            l_imbalance_c.append(imbalance_carnet_t0)

            # 🛡️ BLINDAGE ANTI-DOUBLON & SYNCHRONISATION CSV RAMDISK/DISQUE
            if action not in historique_volumes_qte or historique_volumes_qte[action] == 0:
                for _src_csv in (FICHIER_HISTO_RAMDISK, FICHIER_HISTO_SEANCE):
                    if os.path.exists(_src_csv):
                        try:
                            _df_rec = pd.read_csv(_src_csv, sep=";", usecols=["Ticker", "T0_Qte_Seance", "T0_Volume_Seance"], low_memory=False)
                            _sub_rec = _df_rec[_df_rec["Ticker"].astype(str).str.strip().str.upper() == action_cle]
                            if not _sub_rec.empty:
                                _q_max = int(pd.to_numeric(_sub_rec["T0_Qte_Seance"], errors="coerce").fillna(0).max())
                                _v_max = float(pd.to_numeric(_sub_rec["T0_Volume_Seance"], errors="coerce").fillna(0.0).max())
                                if _q_max > historique_volumes_qte.get(action, 0):
                                    historique_volumes_qte[action] = _q_max
                                    historique_volumes_montant[action] = _v_max
                        except Exception:
                            pass

            prev_qte_validee = int(historique_volumes_qte.get(action, 0))
            prev_vol_valide = float(historique_volumes_montant.get(action, 0.0))

            if qte_totale_seance <= prev_qte_validee:
                delta_qte = 0
                delta_vol = 0.0
            else:
                delta_qte = int(qte_totale_seance - prev_qte_validee)
                ecart_cap = round(float(vol_argent_seance - prev_vol_valide), 3)
                delta_vol = ecart_cap if ecart_cap > 0 else round(delta_qte * dernier_c, 3)
                historique_volumes_qte[action] = int(qte_totale_seance)
                historique_volumes_montant[action] = float(vol_argent_seance)
            
            t1_memo = registre_RAM_t_moins_1.get(action, {})
            emp_t0 = f"{q_achat_t0}_{p_achat_t0}_{p_vente_t0}_{q_vente_t0}"
            emp_t1 = f"{t1_memo.get('T0_Qt_A', '0')}_{t1_memo.get('T0_Achat', '0')}_{t1_memo.get('T0_Vente', '0')}_{t1_memo.get('T0_Qt_V', '0')}" if t1_memo else ""
            
            h_clean = heure_brute_titre.strip()
            if regime_actif in ("ETE", "RAMADAN"):
                est_fenetre_ouv = "08:00:00" <= h_clean < "09:00:00"
                est_fenetre_clot = "12:00:00" <= h_clean < "12:05:00"
                est_fenetre_blocs = "12:15:00" <= h_clean <= "13:00:00"
            else:
                est_fenetre_ouv = "08:00:00" <= h_clean < "09:00:00"
                est_fenetre_clot = "14:00:00" <= h_clean < "14:05:00"
                est_fenetre_blocs = "14:15:00" <= h_clean <= "15:00:00"
            # ==============================================================================
            # 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
            # # Fichier : bourse_macsa.py -> def analyser_boucle_microstructure() (PARTIE 3 DE 3)
            # # SCellage : Raccordement final sans écrasement (CPU à 0% en Hors-Séance)
            # ==============================================================================
            if (est_fenetre_ouv or est_fenetre_clot or est_fenetre_blocs) and (groupe_attribue == 11):
                est_carnet_modifie = (emp_t0 != emp_t1) and (emp_t1 != "")
            else:
                est_carnet_modifie = False

            if (delta_qte > 0 and dernier_c > 0) or est_carnet_modifie:
                if delta_qte > 0 and dernier_c > 0:
                    a_trouve_un_trade, courbe_txt, la_action = exécuter_moteur_lee_ready(
                        action, delta_qte, delta_vol, dernier_c, state_num_calcule, 
                        phase_reg, p_achat_t0, p_vente_t0, qte_totale_seance, 
                        vol_argent_seance, alerte_seuil_active, imbalance_carnet_t0, 
                        spread_pourcent_t0, variation_seance, cours_reference_seance, 
                        row, a_trouve_un_trade, heure_brute_titre, courbe_txt
                    )
                else:
                    if est_fenetre_ouv: la_action, char_ind = "ACCUMULATION", "A"
                    elif est_fenetre_clot: la_action, char_ind = "FIXING_CLOT", "F"
                    else: la_action, char_ind = "BLOC_MOD", "B"
                        
                    courbe_loc = historique_courbe_ticks.get(action, [])
                    courbe_loc.append(char_ind)
                    if len(courbe_loc) > 6: courbe_loc.pop(0)
                    jauge_txt = "".join(courbe_loc).ljust(6, "_")
                    courbe_txt = f"{char_ind} STATE  [{jauge_txt}]"
                    
                    jeton_passif_local = {
                        "Ticker": action, "Heure_Marche": heure_brute_titre, "Heure_Python": heure_machine_formatee,
                        "Nature_Operation": la_action, "Delta_Qte": 0, "Prix_Execution": float(dernier_c), "Delta_Vol": 0.0,
                        "T_Phase_Reglementaire": str(phase_reg), "T_Alerte_Volatilis": "BOOK_CHG", "T_Alerte_Seuil": str(alerte_seuil_active), "T_Graph_Flash": str(jauge_txt),
                        "T0_CMP": round(float(convert_num(row.get("CMP", dernier_c))), 3), "T0_Etat": str(phase_reg), "T0_Qt_A": str(q_achat_t0), "T0_Achat": str(p_achat_t0),
                        "T0_Vente": str(p_vente_t0), "T0_Qt_V": str(q_vente_t0), "T0_Cloture": str(row.get("Clôture", "0")), "T0_CTO": str(row.get("CTO", "0")),
                        "T0_VTO": str(row.get("VTO", "0")), "T0_Ouverture": str(row.get("Ouverture", "0")), "T0_Dernier": str(row.get("Dernier", "0")), "T0_Var": str(row.get("Var.", "0")),
                        "T0_CMP_Brut": f"{convert_num(row.get('CMP', dernier_c)):.3f}", "T0_Qte_Seance": int(qte_totale_seance), "T0_Volume_Seance": round(float(vol_argent_seance), 3), "T0_Heure_Marche": heure_brute_titre,
                        "T1_Etat": str(t1_memo.get("T0_Etat", "CLOTURE Marche ferme")), "T1_Qt_A": str(t1_memo.get("T0_Qt_A", "0")), "T1_Achat": str(t1_memo.get("T0_Achat", "0")),
                        "T1_Vente": str(t1_memo.get("T0_Vente", "0")), "T1_Qt_V": str(t1_memo.get("T0_Qt_V", "0")), "T1_Cloture": str(t1_memo.get("T0_Cloture", "0")),
                        "T1_CTO": str(t1_memo.get("T0_CTO", "0")), "T1_VTO": str(t1_memo.get("T0_VTO", "0")), "T1_Ouverture": str(t1_memo.get("T0_Ouverture", "0")),
                        "T1_Dernier": str(t1_memo.get("T0_Dernier", "0")), "T1_Var": str(t1_memo.get("T0_Var", "0")), "T1_CMP": str(t1_memo.get("T0_CMP", "0")),
                        "T1_Qte_Seance": int(t1_memo.get("T0_Qte_Seance", 0)), "T1_Volume_Seance": round(float(t1_memo.get("T0_Volume_Seance", 0.0)), 3)
                    }
                    registre_transactions_RAM.append(jeton_passif_local.copy())
            else: 
                la_action = "VEILLE"
                
            listes_microstructure.append(la_action)
            status_volatil = "🔲 Calme"
            if state_num_calcule == 9: 
                status_volatil = "⚠️ RÉSERVÉ HAUSSE" if dernier_c > cours_reference_seance else "⚠️ RÉSERVÉ BAISSE"
            elif delta_qte >= 8000: 
                status_volatil = "⚡ PIC DE VOLUME"
            liste_volatiles.append(status_volatil)

            if delta_qte > 0 and dernier_c > 0: 
                historique_dernier_cours[action] = dernier_c
            if action not in cumul_qte_achat: 
                cumul_qte_achat[action] = 0
                cumul_vol_achat[action] = 0.0
                cumul_qte_vente[action] = 0
                cumul_vol_vente[action] = 0.0
# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> def analyser_boucle_microstructure() (PARTIE 3 DE 3 CORRIGÉE)
# # SCellage : Raccordement final synchrone avec injection explicite de l heure maître
# ==============================================================================
            if delta_qte > 0 and la_action in ["ACHAT", "VENTE"]:
                if la_action == "ACHAT": 
                    cumul_qte_achat[action] += delta_qte
                    cumul_vol_achat[action] += delta_vol
                elif la_action == "VENTE": 
                    cumul_qte_vente[action] += delta_qte
                    cumul_vol_vente[action] += delta_vol
            
            l_qte_a.append(int(cumul_qte_achat[action]))
            l_vol_a.append(float(cumul_vol_achat[action]))
            l_qte_v.append(int(cumul_qte_vente[action]))
            l_vol_v.append(float(cumul_vol_vente[action]))
            l_qte_i.append(int(qte_totale_seance - (cumul_qte_achat[action] + cumul_qte_vente[action])))
            l_vol_i.append(float(vol_argent_seance - (cumul_vol_achat[action] + cumul_vol_vente[action])))
            
            volumes_seance[action] = qte_totale_seance
            capitaux_seance[action] = vol_argent_seance
            historique_volumes_qte[action] = qte_totale_seance
            historique_volumes_montant[action] = vol_argent_seance
            
            registre_RAM_t_moins_1[action] = dict(row)
            registre_RAM_t_moins_1[action]["Etat_Num"] = int(state_num_physique_brut)
            registre_RAM_t_moins_1[action]["Achat"] = str(p_achat_t0)
            registre_RAM_t_moins_1[action]["Vente"] = str(p_vente_t0)
            registre_RAM_t_moins_1[action]["Dernier"] = str(dernier_c)
            registre_RAM_t_moins_1[action]["Heure"] = heure_brute_titre
            registre_RAM_t_moins_1[action]["Imbalance_Carnet"] = float(imbalance_carnet_t0)
            registre_RAM_t_moins_1[action]["Spread_Pourcent"] = float(spread_pourcent_t0)
            registre_RAM_t_moins_1[action]["T0_Qt_A"] = str(q_achat_t0)
            registre_RAM_t_moins_1[action]["T0_Achat"] = str(p_achat_t0)
            registre_RAM_t_moins_1[action]["T0_Vente"] = str(p_vente_t0)
            registre_RAM_t_moins_1[action]["T0_Qt_V"] = str(q_vente_t0)

        if registre_transactions_RAM:
            registre_transactions_RAM = [tx for tx in registre_transactions_RAM if int(tx.get("Delta_Qte", 0)) > 0]

        # 🎯 RE-SCELLAGE DE L INJECTION CHRONOMÉTRIQUE :
        # Association explicite de 'heure_api_verifiee' (maximum absolu) à la clé attendue
        params_propulsion = {
            "df_final_cote": df_final_cote, "liste_phases": liste_phases, 
            "listes_microstructure": listes_microstructure, "liste_volatiles": liste_volatiles, 
            "l_achat": l_achat, "l_vente": l_vente, "l_dernier": l_dernier, "l_variation": l_variation, 
            "l_cloture": l_cloture, "l_cto": l_cto, "l_vto": l_vto, "l_ouverture": l_ouverture, "l_cmp": l_cmp, 
            "l_qte_seance": l_qte_seance, "l_vol_seance": l_vol_seance, "l_p_haut": l_p_haut, "l_p_bas": l_p_bas, 
            "l_sh": l_sh, "l_sb": l_sb, "l_dh": l_dh, "l_db": l_db, "l_qte_a": l_qte_a, "l_vol_a": l_vol_a, 
            "l_qte_v": l_qte_v, "l_vol_v": l_vol_v, "l_qte_i": l_qte_i, "l_vol_i": l_vol_i, "l_spread_p": l_spread_p, 
            "l_imbalance_c": l_imbalance_c, "a_trouve_un_trade": a_trouve_un_trade, "l_heure_brute_api": l_heure_brute_api, 
            "l_heure_calculee_pc": l_heure_calculee_pc, "l_etat_brut_api": l_etat_brut_api, 
            "l_etat_calcule_systeme": l_etat_calcule_systeme, "courbe_txt": courbe_txt, 
            "heure_cible_phase": heure_cible_phase, "debut_calcul": debut_calcul,
            "heure_ticker_brut": heure_api_verifiee
        }
        finaliser_matrice_et_propulser(**params_propulsion)
    except Exception as e: 
        print(f"❌ Erreur générale de traitement de la boucle microstructurelle : {e}")

if 'REGIME_ACTIF_SIMU' not in globals():
    try:
        # On utilise votre fonction de la Partie 7 pour fixer le flag au démarrage
        REGIME_ACTIF_SIMU = detecter_regime_horaire(datetime.date.today())
    except:
        # En cas d'appel précoce avant lecture de la fonction, repli d'usine
        REGIME_ACTIF_SIMU = "HIVER"

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : RECONSTITUTIONNEUR MATRICIEL GLOBAL SBE LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (PARTIE 1 DE 2)
# # SCellage : Ingestion d autorité absolue des 14 colonnes manquantes dans l index
# ==============================================================================
if 'global_dernier_ticker_trade' not in globals():
    global global_dernier_ticker_trade, global_dernier_ticker_book, registre_RAM_BBO_passe
    global_dernier_ticker_trade = "SFBT"
    global_dernier_ticker_book = "BNA"
    registre_RAM_BBO_passe = {}


# ==============================================================================
#  eagles STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (BLOC 1 DE 2 CORRIGÉ)
# # SCellage : Extraction forte depuis kwargs contre l exception de nom variable
# ==============================================================================
def finaliser_matrice_et_propulser(**kwargs):
    global registre_transactions_RAM, derniere_action_traitee, FICHIER_WIDGET_TXT, FICHIER_TEXTE_GRILLE, LISTE_VALEURS_UNIFIEES, FICHIER_HISTO_RAMDISK, MODE_PRODUCTION
    global global_dernier_ticker_trade, global_dernier_ticker_book, registre_RAM_BBO_passe, HEURE_CENTRALE_BVMT, REGIME_ACTIF_SIMU
    try:
        action_affichage_secours = "[ PAS DE FLUX ]"
        str_radar_alerte = "🔲 Ingestion SBE Falcon connectée. Flux unifié stable."

        df_final_cote = kwargs["df_final_cote"]
        liste_phases = kwargs["liste_phases"]
        listes_microstructure = kwargs["listes_microstructure"]
        liste_volatiles = kwargs["liste_volatiles"]
        l_achat, l_vente, l_dernier = kwargs["l_achat"], kwargs["l_vente"], kwargs["l_dernier"]
        l_variation, l_cloture = kwargs["l_variation"], kwargs["l_cloture"]
        l_cto, l_vto, l_ouverture, l_cmp = kwargs["l_cto"], kwargs["l_vto"], kwargs["l_ouverture"], kwargs["l_cmp"]
        l_qte_seance, l_vol_seance = kwargs["l_qte_seance"], kwargs["l_vol_seance"]
        l_p_haut, l_p_bas = kwargs["l_p_haut"], kwargs["l_p_bas"]
        l_sh, l_sb, l_dh, l_db = kwargs["l_sh"], kwargs["l_sb"], kwargs["l_dh"], kwargs["l_db"]
        l_qte_a, l_vol_a, l_qte_v, l_vol_v = kwargs["l_qte_a"], kwargs["l_vol_a"], kwargs["l_qte_v"], kwargs["l_vol_v"]
        l_qte_i, l_vol_i = kwargs["l_qte_i"], kwargs["l_vol_i"]
        l_spread_p, l_imbalance_c = kwargs["l_spread_p"], kwargs["l_imbalance_c"]
        a_trouve_un_trade = kwargs["a_trouve_un_trade"]
        l_heure_brute_api = kwargs["l_heure_brute_api"]
        l_heure_calculee_pc = kwargs["l_heure_calculee_pc"]
        l_etat_brut_api = kwargs["l_etat_brut_api"]
        l_etat_calcule_systeme = kwargs["l_etat_calcule_systeme"]
        courbe_txt = kwargs["courbe_txt"]
        heure_cible_phase = kwargs["heure_cible_phase"]
        debut_calcul = kwargs["debut_calcul"]
        
        # 🟢 RESOLUTION DE LA PORTÉE PAR EXTRACTION DES KWARGS :
        # Récupère l'horloge maître passée par le dictionnaire, ou se replie sur la variable globale
        heure_ticker_brut = kwargs.get("heure_ticker_brut", HEURE_CENTRALE_BVMT)

        df_final = pd.DataFrame(index=range(len(LISTE_VALEURS_UNIFIEES)))
        df_final["SbeMarket SheetValeur"] = LISTE_VALEURS_UNIFIEES
        df_final["Etat"] = l_etat_brut_api
        df_final["Qt. A"] = df_final_cote["Qt. A"].values
        df_final["Achat"] = l_achat
        df_final["Vente"] = l_vente
        df_final["Qt.V"] = df_final_cote["Qt.V"].values
        df_final["Clôture"] = l_cloture
        df_final["CTO"] = l_cto
        df_final["VTO"] = l_vto
        df_final["Ouverture"] = l_ouverture
        df_final["Dernier"] = l_dernier
        df_final["Var."] = l_variation
        df_final["CMP"] = l_cmp
        df_final["Qté"] = l_qte_seance
        df_final["Volume"] = l_vol_seance
        df_final["P. Haut"] = l_p_haut
        df_final["P. Bas"] = l_p_bas
        df_final["Seuil.S. Haut"] = l_sh
        df_final["Seuil.S. Bas"] = l_sb
        df_final["Seuil.D. Haut"] = l_dh
        df_final["Seuil.D. Bas"] = l_db
        df_final["Heure"] = l_heure_brute_api
        
        df_final["Cumul_Qte_Achat"] = l_qte_a
        df_final["Cumul_Vol_Achat"] = l_vol_a
        df_final["Cumul_Qte_Vente"] = l_qte_v
        df_final["Cumul_Vol_Vente"] = l_vol_v
        
        l_diff_qte = [int(l_qte_a[i] - l_qte_v[i]) for i in range(len(l_qte_a))]
        l_diff_vol = [float(l_vol_a[i] - l_vol_v[i]) for i in range(len(l_vol_a))]
        
        l_cmp_diff = []
        for i in range(len(l_diff_qte)):
            try:
                denom_loc = abs(l_diff_qte[i])
                if denom_loc > 0:
                    l_cmp_diff.append(round(abs(l_diff_vol[i]) / denom_loc, 3))
                else:
                    l_cmp_diff.append(l_dernier[i])
            except:
                l_cmp_diff.append(l_dernier[i])
                
        df_final["Diff_Qte_Nette"] = l_diff_qte
        df_final["Diff_Vol_Nette"] = l_diff_vol
        df_final["CMP_Diff_Nette"] = l_cmp_diff
        df_final["Spread_Millimes"] = ((df_final["Vente"] - df_final["Achat"]) * 1000).fillna(0).astype(int)
        df_final["Spread_Pourcent"] = l_spread_p
        df_final["Imbalance_Carnet"] = l_imbalance_c
        
        l_tendance_veille = []
        l_tendance_ouv = []
        for i in range(len(l_dernier)):
            l_tendance_veille.append("HAUSSE/VEILLE" if l_dernier[i] > l_cloture[i] else ("BAISSE/VEILLE" if l_dernier[i] < l_cloture[i] else "STABLE/VEILLE"))
            ref_ouv = l_ouverture[i] if l_ouverture[i] > 0 else l_cloture[i]
            l_tendance_ouv.append("HAUSSE/OUV" if l_dernier[i] > ref_ouv else ("BAISSE/OUV" if l_dernier[i] < ref_ouv else "STABLE/OUV"))
            
        df_final["Tendance_Veille"] = l_tendance_veille
        df_final["Tendance_Ouverture"] = l_tendance_ouv
        
        heures_reprise_dynamiques = []
        for idx_act, act_name in enumerate(LISTE_VALEURS_UNIFIEES):
            info_gel_act = registre_reservation_groupe11.get(act_name, {})
            if info_gel_act.get("en_gel") and info_gel_act.get("fin_gel_dt"):
                heures_reprise_dynamiques.append(info_gel_act["fin_gel_dt"].strftime("%H:%M:%S"))
            else:
                heures_reprise_dynamiques.append(heure_cible_phase if heure_cible_phase else "00:00:00")
                
        df_final["Heure_Reprise_Calcul"] = heures_reprise_dynamiques
        df_final["Phase_Reglementaire"] = liste_phases
        df_final["Nature_Operation"] = listes_microstructure
        df_final["Volatiles_Alertes"] = liste_volatiles
        df_final["Etat calculé"] = l_etat_calcule_systeme
        df_final["Heure calculée"] = l_heure_calculee_pc

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : RECONSTITUTIONNEUR MATRICIEL GLOBAL LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (BLOC 2 DE 2)
# # SCellage : Fixation micrométrique de l heure maximale en Colonne 22 (V)
# ==============================================================================
        df_incidents = df_final[df_final["SbeMarket SheetValeur"] != "SYS_MODE_CONFIG"]
        if not df_incidents.empty:
            df_reserve = df_incidents[df_incidents["Etat calculé"] == 9]
            if not df_reserve.empty:
                liste_valeurs_gel = df_reserve["SbeMarket SheetValeur"].tolist()
                str_radar_alerte = f"⚠️ ALERTE MICROSTRUCTURELLE : Réservation active sur {', '.join(liste_valeurs_gel)}"
            else:
                df_tri_var = df_incidents.copy()
                df_tri_var["Var_Abs"] = df_tri_var["Var."].apply(lambda x: abs(convert_num(x)))
                df_tri_var = df_tri_var.sort_values(by="Var_Abs", ascending=False)
                
                if len(df_tri_var) > 0 and float(df_tri_var["Var_Abs"].iloc[0]) >= 3.0:
                    str_radar_alerte = f"🔥 FORTE PRESSION : Anomalie détectée sur {df_tri_var['SbeMarket SheetValeur'].iloc[0]} ({df_tri_var['Var.'].iloc[0]}%)"

        ORDRE_COLONNES_VERROUILLES = [
            "SbeMarket SheetValeur", "Etat", "Qt. A", "Achat", "Vente", "Qt.V", "Clôture", "CTO", "VTO", "Ouverture", 
            "Dernier", "Var.", "CMP", "Qté", "Volume", "P. Haut", "P. Bas", "Seuil.S. Haut", "Seuil.S. Bas", "Seuil.D. Haut", 
            "Seuil.D. Bas", "Heure", "Cumul_Qte_Achat", "Cumul_Vol_Achat", "Cumul_Qte_Vente", "Cumul_Vol_Vente", 
            "Diff_Qte_Nette", "Diff_Vol_Nette", "CMP_Diff_Nette", "Spread_Millimes", "Spread_Pourcent", "Imbalance_Carnet", 
            "Phase_Reglementaire", "Nature_Operation", "Tendance_Veille", "Tendance_Ouverture", "Heure_Reprise_Calcul", "Volatiles_Alertes",
            "Etat calculé", "Heure calculée"
        ]

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : RECONSTITUTIONNEUR MATRICIEL GLOBAL LIVE
# # Fichier : bourse_macsa.py -> finaliser_matrice_et_propulser() (BLOC 2 DE 2 CORRIGÉ)
# # SCellage : Rapatriement de l horloge maximale brute ajustée en Colonne 22
# ==============================================================================
        df_final_ordonne = df_final[ORDRE_COLONNES_VERROUILLES].copy()

        if a_trouve_un_trade and 'derniere_action_traitee' in globals() and 'Ticker' in derniere_action_traitee:
            global_dernier_ticker_trade = str(derniere_action_traitee.get("Ticker", global_dernier_ticker_trade)).strip().upper()
            
        dict_instant_indexe = df_final_ordonne.set_index("SbeMarket SheetValeur").to_dict(orient='index')
        for v_name in LISTE_VALEURS_UNIFIEES:
            v_net = str(v_name).strip().upper()
            row_instant_json = dict_instant_indexe.get(v_net)
            if row_instant_json is not None:
                emp_precedente = registre_RAM_BBO_passe.get(v_net, "0_0_0_0")
                emp_courante = f"{row_instant_json.get('Qt. A', '0')}_{row_instant_json.get('Achat', '0')}_{row_instant_json.get('Vente', '0')}_{row_instant_json.get('Qt.V', '0')}"
                if emp_courante != emp_precedente and emp_precedente != "0_0_0_0":
                    global_dernier_ticker_book = v_net
                    registre_RAM_BBO_passe[v_net] = emp_courante
                    break 
                if emp_precedente == "0_0_0_0":
                    registre_RAM_BBO_passe[v_net] = emp_courante

        # 🎯 CONFORMITÉ CHRONOMÉTRIQUE ROYALE MICROSTRUCTURELLE MAC SA
        mode_detecte_str = "PRODUCTION_REEL" if MODE_PRODUCTION else "SIMULATION_VIRTUELLE"
        ligne_config = {col: "0" for col in ORDRE_COLONNES_VERROUILLES}
        ligne_config["SbeMarket SheetValeur"] = "SYS_MODE_CONFIG"
        ligne_config["Var."] = str(mode_detecte_str)
        
        # 🟢 Colonne 2 (B) : Conserve sa fonction d'identification BBO d'accumulation
        ligne_config["Etat"] = str(global_dernier_ticker_book)
        
        # 🟢 CORRECTIF CHRONOMÉTRIQUE DE PLACE (COLONNE 22 / V) :
        # Au lieu d'extraire la variable globale écrasée à 15:00:00 par la fermeture,
        # on injecte d'autorité l'argument réel ajusté 'heure_ticker_brut' issu directement
        # du dépistage vectoriel de la fonction de récupération, garantissant le live continu.
        ligne_config["Heure"] = str(heure_ticker_brut)
        
        ligne_config["Nature_Operation"] = str(global_dernier_ticker_trade)
        ligne_config["Tendance_Veille"] = f"{global_dernier_ticker_book}*{global_dernier_ticker_trade}*{heure_ticker_brut}"
        ligne_config["Volatiles_Alertes"] = str(REGIME_ACTIF_SIMU)
        ligne_config["Phase_Reglementaire"] = str(str_radar_alerte)
        
        df_flag = pd.DataFrame([ligne_config])
        df_final_ordonne = pd.concat([df_final_ordonne, df_flag], ignore_index=True)

        # Calcul autonome de l'affichage de secours basé strictly sur l'activité microstructurelle
        action_affichage_secours = global_dernier_ticker_trade if str(heure_ticker_brut).replace(":", "").strip()[:4] >= "0900" else global_dernier_ticker_book

        compiler_et_sauvegarder_fichiers(
            df_final_cote, df_final_ordonne, liste_phases, listes_microstructure, 
            l_qte_a, l_vol_a, l_qte_v, l_vol_v, l_qte_i, l_vol_i, l_spread_p, l_imbalance_c, 
            a_trouve_un_trade, heure_ticker_brut, l_heure_calculee_pc[-1], courbe_txt, 
            int(l_etat_calcule_systeme[-1]), heure_cible_phase, action_affichage_secours, str_radar_alerte, 
            ORDRE_COLONNES_VERROUILLES, debut_calcul
        )

    except Exception as e: 
        print(f"❌ Erreur sous-compilation structurelle de la matrice : {e}")
# === FIN DE LA PROCÉDURE INTERNE EXHAUSTIVE ET SCELLÉE ===

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : PARSEUR DE PROPULSION ET GESTION DES FLUSHERS
# # Fichier : bourse_macsa.py -> compiler_et_sauvegarder_fichiers() [CORRIGÉ]
# # SCellage : Restauration de l orthographe 'empreinte_actuelle' et persistance OLE
# ==============================================================================
def compiler_et_sauvegarder_fichiers(df_final_cote, df_final_ordonne, liste_phases, listes_microstructure, l_qte_a, l_vol_a, l_qte_v, l_vol_v, l_qte_i, l_vol_i, l_spread_p, l_imbalance_c, a_trouve_un_trade, heure_ticker_brut, heure_ticker_pc, courbe_txt, id_phase_calculee, heure_cible_phase, action_affichage_secours, str_radar_alerte, ORDRE_COLONNES_VERROUILLES, debut_calcul):
    global registre_transactions_RAM, FICHIER_HISTO_SEANCE, FICHIER_HISTO_RAMDISK, FICHIER_TEXTE_GRILLE, FICHIER_SYNTHESE_CLOTURE, FICHIER_ETAT_RAM, EST_ARCHIVE_FINALE_ENGAGEE, MODE_PRODUCTION
    try:
        maintenant_obj = datetime.datetime.now()
        est_un_jour_ouvrable = maintenant_obj.weekday() < 5
        autoriser_ecriture_histo = est_un_jour_ouvrable or (not MODE_PRODUCTION)

        try:
            str_dernier = df_final_ordonne['Dernier'].astype(str).str.cat()
            str_qte = df_final_ordonne['Qté'].astype(str).str.cat()
            str_achat = df_final_ordonne['Achat'].astype(str).str.cat()
            str_vente = df_final_ordonne['Vente'].astype(str).str.cat()
            empreinte_actuelle = f"{str_dernier}_{str_qte}_{str_achat}_{str_vente}"
        except:
            empreinte_actuelle = str(heure_ticker_brut)

        empreinte_precedente = historique_phase_precedente.get("EMPREINTE_GRILLE_RAM", "")
        
        # Mise à jour de DonneesSource.txt (toujours active pour afficher l'état du marché dans Excel)
        if a_trouve_un_trade or empreinte_actuelle != empreinte_precedente or heure_ticker_pc != historique_phase_precedente.get("DERNIER_PC_TICK", ""):
            try:
                historique_phase_precedente["DERNIER_PC_TICK"] = heure_ticker_pc
                historique_phase_precedente["EMPREINTE_GRILLE_RAM"] = empreinte_actuelle
                with open(FICHIER_TEXTE_GRILLE, "w", encoding="utf-8-sig", errors="replace") as f_grille:
                    df_final_ordonne[ORDRE_COLONNES_VERROUILLES].to_csv(f_grille, sep="\t", index=False, lineterminator="\n")
            except:
                pass

        # Journalisation au fil de l'eau du grand livre de séance (BLOQUÉE LE WEEK-END EN PRODUCTION)
        if registre_transactions_RAM:
            try:
                if autoriser_ecriture_histo:
                    df_histo = pd.DataFrame(registre_transactions_RAM)
                    df_histo["Date_Seance"] = datetime.date.today().strftime("%d/%m/%Y")
                    df_histo = df_histo.dropna(how="all")
                    df_histo = df_histo[pd.to_numeric(df_histo["Delta_Qte"], errors="coerce").fillna(0) > 0]
                    df_histo = df_histo.drop_duplicates(subset=["Ticker", "T0_Qte_Seance"], keep="first")

                    if not df_histo.empty:
                        df_histo = df_histo.sort_values(by="Heure_Marche", ascending=True, kind="mergesort")
                        df_histo.to_csv(
                            FICHIER_HISTO_RAMDISK, 
                            mode='a', 
                            header=not os.path.exists(FICHIER_HISTO_RAMDISK), 
                            sep=';', 
                            index=False, 
                            encoding='utf-8', 
                            lineterminator="\n"
                        )
                registre_transactions_RAM.clear()
            except Exception as e_tri_log:
                print(f"⚠️ Alerte microstructurelle de tri chronologique : {e_tri_log}")

        # Verrou intra-séance préventif de minute "01" (UNIQUEMENT JOURS OUVRABLES)
        minute_courante_str = heure_ticker_pc[3:5]
        cle_verrou_heure = f"FLUSH_DONE_{heure_ticker_pc[:2]}"
        est_dans_la_fenetre_seance = "08:00:00" <= heure_ticker_pc <= "18:00:00"
        
        if est_un_jour_ouvrable and est_dans_la_fenetre_seance and minute_courante_str == "01" and not historique_phase_precedente.get(cle_verrou_heure, False):
            if os.path.exists(FICHIER_HISTO_RAMDISK) and MODE_PRODUCTION:
                try:
                    df_total_seance = pd.read_csv(FICHIER_HISTO_RAMDISK, sep=";", low_memory=False)
                    df_total_seance = df_total_seance.dropna(how='all')
                    if not df_total_seance.empty:
                        df_total_seance.to_csv(FICHIER_HISTO_SEANCE, sep=';', index=False, encoding='utf-8', lineterminator="\n")
                        print(f"\n[🛡️ FLUSHER INTRA-SÉANCE] Sécurité exécutée à {heure_ticker_pc} : Grand livre sauvegardé.")
                    historique_phase_precedente[cle_verrou_heure] = True
                except:
                    pass

        # Gestion comptable du Closing de Place (15h00) — BLOQUÉE LE WEEK-END
        h_clean_brut_test = str(heure_ticker_brut).replace(":", "").strip()[:4]
        if est_un_jour_ouvrable and h_clean_brut_test >= "1500" and not EST_ARCHIVE_FINALE_ENGAGEE:
            try:
                if os.path.exists(FICHIER_HISTO_RAMDISK):
                    if MODE_PRODUCTION:
                        df_ramdisk = pd.read_csv(FICHIER_HISTO_RAMDISK, sep=";", low_memory=False)
                        df_ramdisk = df_ramdisk.dropna(how='all')
                        df_ramdisk.to_csv(FICHIER_HISTO_SEANCE, sep=';', index=False, encoding='utf-8', lineterminator="\n")
                        print(f"\n[🏁] CLOSING MARCHÉ RÉEL : Données de session consolidées sur disque C:.")
                    
                if MODE_PRODUCTION:
                    df_final_ordonne.to_csv(FICHIER_SYNTHESE_CLOTURE, sep=";", index=False, encoding="utf-8", lineterminator="\n")
                EST_ARCHIVE_FINALE_ENGAGEE = True
            except Exception as e_cloture:
                print(f"⚠️ Échec du transfert de clôture nominale : {e_cloture}")

        # Affichage console synchrone
        heure_systeme_actuelle = time.strftime('%H:%M:%S')
        temps_execution_ms = (time.perf_counter_ns() - debut_calcul) / 1_000_000
        etat_console = "WEEK-END (Veille)" if (not est_un_jour_ouvrable and MODE_PRODUCTION) else "Base de données RAM: OK"
        trame_console = f"[*] [PC: {heure_systeme_actuelle}] | BVMT: {heure_ticker_brut} | Noyau: {temps_execution_ms:.1f}ms | {etat_console}"
        sys.stdout.write(f"\r{trame_console.ljust(110)[:110]}\x1b[K")
        sys.stdout.flush()

        # Sauvegarde JSON de l'état RAM (BLOQUÉE LE WEEK-END EN PRODUCTION)
        if autoriser_ecriture_histo and (os.path.exists(FICHIER_HISTO_RAMDISK) or MODE_PRODUCTION):
            sauvegarder_etat_RAM_disque()

    except Exception as e_main:
        print(f"❌ Erreur sous-traitement d'écriture fichier : {e_main}")




# 🎯 PERSISTANCE JSON RECTIFIÉE SCELLÉE NOMINALE
def sauvegarder_etat_RAM_disque():
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, FICHIER_ETAT_RAM, registre_RAM_t_moins_1, derniere_action_traitee
    try:
        historique_cours_propre = dict(historique_dernier_cours)
        for tk in LISTE_VALEURS_UNIFIEES:
            t1_memo = registre_RAM_t_moins_1.get(tk, {})
            historique_cours_propre[f"{tk}_Imbalance"] = float(t1_memo.get("Imbalance_Carnet", 0.0))
            historique_cours_propre[f"{tk}_Spread"] = float(t1_memo.get("Spread_Pourcent", 0.0))

        dict_miroir_ram = {
            "cumul_qte_achat": dict(cumul_qte_achat), 
            "cumul_vol_achat": dict(cumul_vol_achat),
            "cumul_qte_vente": dict(cumul_qte_vente), 
            "cumul_vol_vente": dict(cumul_vol_vente),
            "cumul_qte_indecis": dict(cumul_qte_indecis), 
            "cumul_vol_indecis": dict(cumul_vol_indecis),
            "historique_volumes_qte": dict(historique_volumes_qte), 
            "historique_volumes_montant": dict(historique_volumes_montant),
            "historique_dernier_cours": historique_cours_propre,
            # Remplacement de l action fictive SFBT par le drapeau de neutralité universel SYS_STANDBY
            "derniere_action_traitee": dict(derniere_action_traitee) if "registre_transactions_RAM" in globals() and (len(registre_transactions_RAM) > 0 or  derniere_action_traitee) else {
                "Ticker": "SYS_STANDBY", "Heure_Marche": HEURE_CENTRALE_BVMT, "Heure_Python": time.strftime('%H:%M:%S'),
                "Nature_Operation": "VEILLE", "Delta_Qte": 0, "Prix_Execution": 0.0, "Delta_Vol": 0.0,
                "T_Phase_Reglementaire": "CLOTURE Marche ferme", "T0_CMP": 0.0, "Tendance_Veille": "STABLE",
                "Tendance_Ouverture": "STABLE", "Heure_Reprise_Calcul": "00:00:00"
            }
        }
        with open(FICHIER_ETAT_RAM, "w", encoding="utf-8") as f_json:
            json.dump(dict_miroir_ram, f_json, indent=4, ensure_ascii=False)
    except:
        pass

# Initialisation de la grille maîtresse permanente en mémoire vive active
df_master_ram_cote = None

# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : MOTEUR ASYNCHRONE D ASPIRATION FALCON DESENCLAVÉ
# # Fichier : bourse_macsa.py -> boucle_background_aspiration_falcon()
# # SCellage : Élargissement du timeout à 3.0s et initialisation d autorité du tampon (0% Gel)
# ==============================================================================
def boucle_background_aspiration_falcon():
    global BUFFER_TEXTE_RESEAU_FALCON, URL_API, FICHIER_SIMULE_SOURCE, MODE_SIMULATION_ACTIF, VERROU_PARTAGE_THREAD
    
    # 🎯 AMORCE DE CHARGE DE SÉCURITÉ : Évite que le buffer ne démarre à blanc
    with VERROU_PARTAGE_THREAD:
        if BUFFER_TEXTE_RESEAU_FALCON == "":
            BUFFER_TEXTE_RESEAU_FALCON = "SbeMarket SheetValeur;Etat;Qt. A;Achat;Vente;Qt.V;Clôture;CTO;VTO;Ouverture;Dernier;Var.;CMP;Qté;Volume;P. Haut;P. Bas;Seuil.S. Haut;Seuil.S. Bas;Seuil.D. Haut;Seuil.D. Bas;Heure\n"

    while True:
        try:
            if MODE_SIMULATION_ACTIF:
                # --- MODE SIMULATION : Lecture asynchrone du grand livre de simulation ---
                if os.path.exists(FICHIER_SIMULE_SOURCE) and os.path.getsize(FICHIER_SIMULE_SOURCE) > 500:
                    with open(FICHIER_SIMULE_SOURCE, "r", encoding="latin1") as f_sim:
                        txt = f_sim.read()
                    with VERROU_PARTAGE_THREAD:
                        BUFFER_TEXTE_RESEAU_FALCON = txt
                time.sleep(0.01)
            else:
                # --- MARCHE REEL LIVE : Interrogation OLE du serveur Falcon avec Timeout assoupli ---
                # Le passage à timeout=3.0 permet d absorber le retard de handshake à l allumage
                hdrs_sbe = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    "Accept": "text/plain, text/html, */*",
                    "Cache-Control": "no-cache"
                }
                reponse = requests.get(URL_API, headers=hdrs_sbe, verify=False, timeout=10.0)
                if reponse.status_code == 200:
                    reponse.encoding = 'latin1'
                    with VERROU_PARTAGE_THREAD:
                        BUFFER_TEXTE_RESEAU_FALCON = reponse.text
                    if not hasattr(boucle_background_aspiration_falcon, "_ok_logged"):
                        boucle_background_aspiration_falcon._ok_logged = True
                        print(f"[✅ SBE-LIVE] Flux MAC SA connecté ({len(reponse.text)} octets reçus).", flush=True)
                else:
                    now_t = time.time()
                    if now_t - getattr(boucle_background_aspiration_falcon, "_last_err_t", 0) > 15:
                        boucle_background_aspiration_falcon._last_err_t = now_t
                        print(f"[⚠️ SBE-HTTP] Statut {reponse.status_code} depuis {URL_API}", flush=True)
                time.sleep(0.05)
        except Exception as e_net:
            now_t = time.time()
            if now_t - getattr(boucle_background_aspiration_falcon, "_last_err_t", 0) > 15:
                boucle_background_aspiration_falcon._last_err_t = now_t
                print(f"[❌ SBE-NET-ERR] Échec connexion {URL_API} : {e_net}", flush=True)
            time.sleep(0.2)

def executer_requete_sbe_marche_live():
    global BUFFER_TEXTE_RESEAU_FALCON, VERROU_PARTAGE_THREAD
    # Sécurisation de l armement automatique du thread asynchrone d arriere-plan
    if not hasattr(executer_requete_sbe_marche_live, "thread_lance"):
        executer_requete_sbe_marche_live.thread_lance = True
        t_bg = threading.Thread(target=boucle_background_aspiration_falcon, daemon=True)
        t_bg.start()
        print("[*] 📡 Thread asynchrone d arriere-plan Falcon SBE initialise (0.1ms principal).")
    with VERROU_PARTAGE_THREAD:
        return BUFFER_TEXTE_RESEAU_FALCON


# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : SYSTÈME DE RÉHYDRATATION POST-COUPURE JSON
# # Fichier : bourse_macsa.py -> restaurer_etat_RAM_depuis_coupure() & coupe-circuit
# # PARTIE 18 / 20 : REPLI EXCLUSIF DU CACHE SANS PERTE DE TYPE (NOMINALE OLE)
# ==============================================================================
def restaurer_etat_RAM_depuis_coupure():
    global cumul_qte_achat, cumul_vol_achat, cumul_qte_vente, cumul_vol_vente, cumul_qte_indecis, cumul_vol_indecis, historique_volumes_qte, historique_volumes_montant, historique_dernier_cours, FICHIER_ETAT_RAM, DATE_JOUR_STR
    if os.path.exists(FICHIER_ETAT_RAM) and os.path.getsize(FICHIER_ETAT_RAM) > 50:
        try:
            if DATE_JOUR_STR not in os.path.basename(FICHIER_ETAT_RAM):
                print(f"[*] [⏳ SÉCURITÉ QUARTZ] Registre JSON obsolète. Nouveau départ à blanc engagé.")
                return

            with open(FICHIER_ETAT_RAM, "r", encoding="utf-8") as f_state:
                etat_global = json.load(f_state)
            
            def epurer_dico_ram(dico_cible):
                dico_propre = {}
                for k, v in dico_cible.items():
                    try:
                        val_str = str(v).replace("[", "").replace("]", "").replace(",", ".").strip()
                        dico_propre[str(k).strip()] = float(val_str) if "." in val_str else int(float(val_str))
                    except:
                        dico_propre[str(k).strip()] = 0.0
                return dico_propre

            cumul_qte_achat.update(epurer_dico_ram(etat_global.get("cumul_qte_achat", {})))
            cumul_vol_achat.update(epurer_dico_ram(etat_global.get("cumul_vol_achat", {})))
            cumul_qte_vente.update(epurer_dico_ram(etat_global.get("cumul_qte_vente", {})))
            cumul_vol_vente.update(epurer_dico_ram(etat_global.get("cumul_vol_vente", {})))
            cumul_qte_indecis.update(epurer_dico_ram(etat_global.get("cumul_qte_indecis", {})))
            cumul_vol_indecis.update(epurer_dico_ram(etat_global.get("cumul_vol_indecis", {})))
            historique_volumes_qte.update(epurer_dico_ram(etat_global.get("historique_volumes_qte", {})))
            historique_volumes_montant.update(epurer_dico_ram(etat_global.get("historique_volumes_montant", {})))
            historique_dernier_cours.update(epurer_dico_ram(etat_global.get("historique_dernier_cours", {})))
            
            nom_env = "SIMULATION" if "simulation" in FICHIER_ETAT_RAM else "MARCHÉ RÉEL LIVE"
            print(f"[*] [🛡️ REPRISE] Écosystème {nom_env} réhydraté avec succès. 77 valeurs chargées.")
        except Exception as e:
            print(f"⚠️ Erreur rechargement d'état RAM après coupure : {e}")

def declencher_coupe_circuit_operateur(signum, frame):
    sys.stdout.write("\n\n[*] 🛑 Arrêt d'urgence de la station quantitative MAC SA exécuté par l'opérateur.\n")
    sys.stdout.flush()
    try:
        global registre_transactions_RAM, FICHIER_HISTO_SEANCE, FICHIER_HISTO_RAMDISK, FICHIER_ETAT_RAM, MODE_PRODUCTION
        
        if registre_transactions_RAM and MODE_PRODUCTION:
            try:
                df_histo = pd.DataFrame(registre_transactions_RAM)
                df_histo["Date_Seance"] = datetime.date.today().strftime("%d/%m/%Y")
                df_histo.to_csv(FICHIER_HISTO_RAMDISK, mode='a', header=not os.path.exists(FICHIER_HISTO_RAMDISK), sep=';', index=False, encoding='utf-8', lineterminator="\n")
                registre_transactions_RAM.clear()
            except: pass
                
        if os.path.exists(FICHIER_HISTO_RAMDISK):
            if MODE_PRODUCTION:
                try:
                    df_ramdisk = pd.read_csv(FICHIER_HISTO_RAMDISK, sep=";", low_memory=False)
                    df_ramdisk.to_csv(FICHIER_HISTO_SEANCE, sep=';', index=False, encoding='utf-8', lineterminator="\n")
                    print(f"[*] [🛡️ SÉCURITÉ REPRISE] Journal d'activité sauvegardé sur disque dur C:.")
                except: pass
            else:
                try: os.remove(FICHIER_HISTO_RAMDISK)
                except: pass
                print(f"[*] [🗑️ NETTOYAGE SIMU] Cache CSV virtuel purgé.")
                
        if not MODE_PRODUCTION:
            if os.path.exists(FICHIER_ETAT_RAM):
                try: os.remove(FICHIER_ETAT_RAM)
                except: pass
            print(r"[*] [🏁] FIN DE SCÉNARIO VIRTUEL : 100% de la mémoire de Place effacée du cache.")

        est_jour_ouvre_arret = datetime.datetime.now().weekday() < 5
        if MODE_PRODUCTION and est_jour_ouvre_arret:

            sauvegarder_etat_RAM_disque()
            
    except: pass
    os._exit(0)


# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU ANALYTIQUE MICROSTRUCTUREL REEL LIVE
# # Fichier : bourse_macsa.py -> def recuperer_et_analyser() [CHRONO ATOS RE-SCELLÉ]
# # SCellage : Extraction vectorielle de la valeur MAXIMALE absolue des horloges (0% Retard)
# ==============================================================================
def recuperer_et_analyser():
    debut_calcul = time.perf_counter_ns()
    global registre_RAM_t_moins_1, MODE_PRODUCTION, HEURE_CENTRALE_BVMT, LISTE_ENTETES_OFFICIELS, df_master_ram_cote, MODE_SIMULATION_ACTIF, LISTE_VALEURS_UNIFIEES, historique_dernier_cours
    
    maintenant_obj = datetime.datetime.now()
    regime_actif = detecter_regime_horaire(maintenant_obj.date())
    text_brut_source = executer_requete_sbe_marche_live()
    
    # 🎯 COUPLAGE ANTI-DISTORSION SUPRÊME :
    # Si le tampon réseau est en attente, on extrait dynamiquement le dernier cours réel 
    # de l'action. Si le dictionnaire RAM est vide, on interroge la mémoire df_master_ram_cote.
    # On élimine définitivement la valeur fixe 10.0 pour interdire tout étirement d'axe Y parasite.
    if not text_brut_source or str(text_brut_source).strip() == "" or len(str(text_brut_source).strip()) < 100:
        text_brut_source = "SbeMarket SheetValeur;Etat;Qt. A;Achat;Vente;Qt.V;Clôture;CTO;VTO;Ouverture;Dernier;Var.;CMP;Qté;Volume;P. Haut;P. Bas;Seuil.S. Haut;Seuil.S. Bas;Seuil.D. Haut;Seuil.D. Bas;Heure\n"
        for tk_fill in LISTE_VALEURS_UNIFIEES:
            px_ref_veille = 0.0
            
            # 1er rideau de sécurité : cours de la session active
            if tk_fill in historique_dernier_cours and historique_dernier_cours[tk_fill] > 0:
                px_ref_veille = historique_dernier_cours[tk_fill]
            
            # 2ème rideau de sécurité : cours de clôture historique en mémoire tampon df_master_ram_cote
            if px_ref_veille <= 0 and df_master_ram_cote is not None and tk_fill in df_master_ram_cote.index:
                try:
                    px_ref_veille = convert_num(df_master_ram_cote.loc[tk_fill, "Clôture"])
                    if px_ref_veille <= 0:
                        px_ref_veille = convert_num(df_master_ram_cote.loc[tk_fill, "Dernier"])
                except:
                    px_ref_veille = 0.0
            
            # Fallback d'usine historique indexé par instrument réel
            if px_ref_veille <= 0:
                if tk_fill == "AMEN BANK": px_ref_veille = 92.000
                elif tk_fill == "SFBT": px_ref_veille = 13.600
                elif tk_fill == "BNA": px_ref_veille = 19.800
                elif tk_fill == "SAH": px_ref_veille = 12.200
                elif tk_fill == "EURO-CYCLES": px_ref_veille = 9.700
                else: px_ref_veille = 10.000
                
            sh_dyn = round(px_ref_veille * 1.06, 3)
            sb_dyn = round(px_ref_veille * 0.94, 3)
            dh_dyn = round(px_ref_veille * 1.03, 3)
            db_dyn = round(px_ref_veille * 0.97, 3)
            text_brut_source += f"{tk_fill};5;0;{px_ref_veille};{px_ref_veille};0;{px_ref_veille};0;0;0;{px_ref_veille};0;{px_ref_veille};0;0;{px_ref_veille};{px_ref_veille};{sh_dyn};{sb_dyn};{dh_dyn};{db_dyn};083000\n"
    
    heure_maximale_detectee_brute = "000000"
    lignes_data = []
    
    lignes_brutes = text_brut_source.split('\n')
    for l in lignes_brutes:
        l_strip = l.strip()
        if not l_strip or ';' not in l_strip: 
            continue
            
        elements = l_strip.split(';')
        if len(elements) >= 22:
            nom_ticker_brut = str(elements[0]).strip().upper()
            
            if "SBEMARKET" not in nom_ticker_brut and "SYS_MODE_CONFIG" not in nom_ticker_brut:
                lignes_data.append(elements[:22])
                
                h_brut = str(elements[21]).strip().replace(":", "")
                if h_brut and h_brut != "000000" and h_brut.isdigit() and len(h_brut) == 6:
                    if int(h_brut) > int(heure_maximale_detectee_brute):
                        heure_maximale_detectee_brute = h_brut

    df_paquet_instantane = pd.DataFrame(lignes_data, columns=LISTE_ENTETES_OFFICIELS) if len(lignes_data) > 0 else pd.DataFrame(columns=LISTE_ENTETES_OFFICIELS)
    df_paquet_instantane["SbeMarket SheetValeur"] = df_paquet_instantane["SbeMarket SheetValeur"].astype(str).str.strip().str.upper()

    heure_limite_cloture = "13:00:00" if regime_actif in ("ETE", "RAMADAN") else "15:00:00"
    derniere_heure_marche_detectee = Hospitalier_max_time(HEURE_CENTRALE_BVMT)
    
    if heure_maximale_detectee_brute != "000000":
        heure_proposee = f"{heure_maximale_detectee_brute[:2]}:{heure_maximale_detectee_brute[2:4]}:{heure_maximale_detectee_brute[4:6]}"
    else:
        heure_proposee = derniere_heure_marche_detectee

    if heure_proposee < Hospitalier_max_time(derniere_heure_marche_detectee):
        HEURE_CENTRALE_BVMT = Hospitalier_max_time(derniere_heure_marche_detectee)
    else:
        HEURE_CENTRALE_BVMT = heure_proposee

    if HEURE_CENTRALE_BVMT >= heure_limite_cloture:
        HEURE_CENTRALE_BVMT = heure_limite_cloture

    filtrer_et_unifier_dataframe_cote(df_paquet_instantane, None, regime_actif, debut_calcul)



def Hospitalier_max_time(heure_str):
    """Sécurise le formatage de la chaîne de persistance sans aucune régression."""
    if not heure_str or str(heure_str).strip() == "" or "--" in str(heure_str):
        return "08:30:00"
    return str(heure_str).strip()





# 🎯 VOTRE LOGIQUE DE FUSION GLOBALE RESTAURÉE MOT À MOT SANS AUCUNE SOUSTRACTION
def filtrer_et_unifier_dataframe_cote(df_actions, df_final_cote, regime_actif, debut_calcul):
    global df_master_ram_cote, LISTE_VALEURS_UNIFIEES, LISTE_ENTETES_OFFICIELS
    
    if df_master_ram_cote is None:
        lignes_initiales = []
        for action in LISTE_VALEURS_UNIFIEES:
            row_init = {col: "0" for col in LISTE_ENTETES_OFFICIELS}
            row_init["SbeMarket SheetValeur"] = action
            row_init["Heure"] = "08:00:00"
            row_init["Etat"] = "5"
            lignes_initiales.append(row_init)
        df_master_ram_cote = pd.DataFrame(lignes_initiales)
        df_master_ram_cote.set_index("SbeMarket SheetValeur", inplace=True, drop=False)

    if df_actions is None or df_actions.empty or "SbeMarket SheetValeur" not in df_actions.columns:
        return

    df_paquet_idx = df_actions.set_index("SbeMarket SheetValeur", drop=False)
    df_master_ram_cote.update(df_paquet_idx)
    df_master_ram_cote["Etat_Num"] = df_master_ram_cote["Etat"].apply(lambda x: int(re.sub(r'[^\d]', '', str(x))) if re.sub(r'[^\d]', '', str(x)) != "" else 5).astype(int)
    
    df_miroir_permute = df_master_ram_cote.copy()
    df_miroir_permute["Achat"], df_miroir_permute["Vente"] = df_miroir_permute["Vente"].copy(), df_miroir_permute["Achat"].copy()
    df_miroir_permute["Qt. A"], df_miroir_permute["Qt.V"] = df_miroir_permute["Qt.V"].copy(), df_miroir_permute["Qt. A"].copy()
    
    # Rappel du curseur microstructurel (Parties 14/15/16/17/18)
    analyser_boucle_microstructure(df_miroir_permute, regime_actif, debut_calcul)



# ==============================================================================
# 🦅 STATION QUANTITATIVE MAC SA : NOYAU D ALLUMAGE TERMINAL ET METRONOME REEL
# # Fichier : bourse_macsa.py -> __main__ sovereign loop [PULSATION RE-SCELLÉE]
# # SCellage : Protection Week-End, Nettoyage Veille > 08h01 et Horloge Maître
# ==============================================================================

# === EXTENSION SERVEUR WEB CLOUD & SAUVEGARDE GITHUB (HUGGING FACE SPACES) ===
import base64
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import unquote, urlparse

GH_TOKEN = os.environ.get("GH_TOKEN", "").strip()
GH_REPO = (os.environ.get("GH_DATA_REPO", "") or os.environ.get("GH_REPO", "")).strip()
_LAST_GH_SYNC = 0.0

def sync_github_cloud():
    global _LAST_GH_SYNC
    tk = os.environ.get("GH_TOKEN", "").strip() or globals().get("GH_TOKEN", "").strip()
    repo = (os.environ.get("GH_DATA_REPO", "") or os.environ.get("GH_REPO", "")).strip() or globals().get("GH_REPO", "").strip()
    # Nettoyer GH_REPO au cas où il contiendrait une URL complète
    repo = repo.replace("https://github.com", "").replace("http://github.com", "").strip("/")
    if not tk or not repo:
        return
    if (time.time() - _LAST_GH_SYNC) < 120:
        return
    _LAST_GH_SYNC = time.time()
    date_jour = datetime.datetime.now().strftime("%d%m%Y")
    headers = {
        "Authorization": f"Bearer {tk}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "CH-WaveCandle-HFT",
        "Content-Type": "application/json"
    }
    for root, _, files in os.walk(_HIST_DIR):
        if "Backup" in root: continue
        for fn in files:
            # Synchroniser uniquement le fichier de la séance du jour (évite de repousser les 38 archives)
            if not fn.endswith(".csv") or date_jour not in fn:
                continue
            fp = os.path.join(root, fn)
            rel = "Historique/" + os.path.relpath(fp, _HIST_DIR).replace("\\", "/")
            api_url = f"https://api.github.com/repos/{repo}/contents/{rel}"
            try:
                sha = None
                try:
                    with urllib.request.urlopen(urllib.request.Request(api_url, headers=headers), timeout=10) as r:
                        sha = json.loads(r.read().decode("utf-8")).get("sha")
                except urllib.error.HTTPError as he:
                    if he.code != 404: raise
                with open(fp, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode("ascii")
                payload = {"message": f"Auto-save BVMT {rel}", "content": b64}
                if sha: payload["sha"] = sha
                urllib.request.urlopen(urllib.request.Request(api_url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="PUT"), timeout=15).read()
                print(f"[☁️ GITHUB-SYNC] {rel} sauvegardé sur GitHub ({os.path.getsize(fp)} octets).", flush=True)
            except Exception as ex_gh:
                print(f"[⚠️ GITHUB-SYNC-ERR] ({api_url}) : {ex_gh}", flush=True)

def boucle_synchro_github():
    time.sleep(10)
    try: sync_github_cloud()
    except Exception: pass
    while True:
        time.sleep(60)
        try: sync_github_cloud()
        except Exception: pass
    while True:
        time.sleep(60)
        try: sync_github_cloud()
        except Exception: pass

class HandlerStationCloud(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_GET(self):
        base = os.path.dirname(os.path.abspath(__file__))
        p = urlparse(self.path).path
        if p in ("/", "/CH-WaveCandle.html"):
            with open(os.path.join(base, "CH-WaveCandle.html"), "rb") as f: data = f.read()
            self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.end_headers(); self.wfile.write(data)
        elif p == "/api/index":
            hf = []
            for root, _, files in os.walk(_HIST_DIR):
                if "Backup" in root: continue
                for fn in files:
                    if fn.endswith(".csv"):
                        hf.append(os.path.relpath(os.path.join(root, fn), ARCHIVE_DIR).replace("\\", "/"))
            lf = []
            if os.path.exists(TEMP_DIR):
                for fn in os.listdir(TEMP_DIR):
                    fp = os.path.join(TEMP_DIR, fn)
                    if os.path.isfile(fp):
                        lf.append({"Name": fn, "Length": os.path.getsize(fp), "Mtime": int(os.path.getmtime(fp) * 1000)})
            body = json.dumps({"hist": sorted(hf), "live": lf, "py": True, "url": globals().get("URL_SBE", "https://macsa.com.tn"), "err": ""}).encode("utf-8")
            self.send_response(200); self.send_header("Content-Type", "application/json; charset=utf-8"); self.end_headers(); self.wfile.write(body)
        elif p.startswith("/files/"):
            rel = unquote(p[7:]).replace("/", os.sep)
            fp = os.path.abspath(os.path.join(base, rel))
            if fp.startswith(base) and os.path.isfile(fp):
                with open(fp, "rb") as f: data = f.read()
                self.send_response(200); self.send_header("Content-Type", "text/plain; charset=utf-8"); self.end_headers(); self.wfile.write(data)
            else:
                self.send_response(404); self.end_headers()
        else:
            self.send_response(404); self.end_headers()

def demarrer_serveur_web():
    port = int(os.environ.get("PORT", "7860"))
    ThreadingHTTPServer(("0.0.0.0", port), HandlerStationCloud).serve_forever()
def lancer_collecteur_bvmt():
    global FICHIER_HISTO_SEANCE, FICHIER_SOURCE, ARCHIVE_DIR, TEMP_DIR
    if sys.platform == "win32":
        import ctypes
        # ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
        # Ordonne au noyau Windows de maintenir les threads CPU actifs même en verrouillage session
        ctypes.windll.kernel32.SetThreadExecutionState(0x80000000 | 0x00000001 | 0x00000040)
        print("[*] 🛡️ SCELLAGE MATÉRIEL : Windows Execution State verrouillé en mode Turbo Éveillé.")
    
    try:
        signal.signal(signal.SIGINT, declencher_coupe_circuit_operateur)
    except ValueError:
        pass
    print("\n================================================================================")
    print("[*] 🚀 Station quantitative globale MAC SA allumée [MOTEUR SBE DIRECT LIVE]")
    print(f"[*] 📂 Base de données d'activité active : {FICHIER_HISTO_SEANCE}")
    print("================================================================================\n")
    
    maintenant_allumage = datetime.datetime.now()
    date_instant_pc = maintenant_allumage.strftime("%d%m%Y")
    est_jour_ouvre_demarrage = maintenant_allumage.weekday() < 5
    
    # Nettoyage immédiat des fichiers de la veille si l'heure PC dépasse 08:01:00
    if maintenant_allumage.strftime("%H:%M:%S") > "08:01:00":
        verifier_et_nettoyer_fichiers_veille(date_instant_pc)
        
    if est_jour_ouvre_demarrage or not MODE_PRODUCTION:
        restaurer_etat_RAM_depuis_coupure()
    else:
        print("[*] 🛑 WEEK-END DÉTECTÉ (Samedi/Dimanche) : Station en mode veille passive (0 écriture disque).")
    
    thread_falcon = threading.Thread(target=boucle_background_aspiration_falcon, daemon=True)
    thread_falcon.start()
    print("[*] 📡 Thread asynchrone de dechargement de latence Falcon active (0.1ms CPU).")
    
    # Synchronisation comptable à froid sur le grand livre uniquement les jours ouvrables
    if MODE_PRODUCTION and est_jour_ouvre_demarrage and os.path.exists(FICHIER_HISTO_SEANCE) and os.path.getsize(FICHIER_HISTO_SEANCE) > 120:
        try:
            df_reprise_flux = pd.read_csv(FICHIER_HISTO_SEANCE, sep=";", low_memory=False)
            if not df_reprise_flux.empty:
                for tk in LISTE_VALEURS_UNIFIEES:
                    df_filtre_action = df_reprise_flux[df_reprise_flux["Ticker"] == tk]
                    if not df_filtre_action.empty:
                        derniere_ligne_historique = df_filtre_action.iloc[-1]
                        val_qte_memo = int(_forcer_float_reprise(derniere_ligne_historique.get("T0_Qte_Seance", 0)))
                        val_vol_memo = _forcer_float_reprise(derniere_ligne_historique.get("T0_Volume_Seance", 0.0))
                        
                        volumes_seance[tk] = val_qte_memo
                        capitaux_seance[tk] = val_vol_memo
                        historique_volumes_qte[tk] = val_qte_memo
                        historique_volumes_montant[tk] = val_vol_memo
                print(f"[*] ✅ Reprise complementaire reussie : {os.path.basename(FICHIER_HISTO_SEANCE)} synchronise a froid.")
            del df_reprise_flux 
        except:
            pass

    REGIME_ACTIF = detecter_regime_horaire(datetime.date.today())
    
    # Calage initial strict à 08:30:00 (empêche le blocage à 15:00:00 lors d'un lancement l'après-midi)
    HEURE_CENTRALE_BVMT = "08:30:00"
    
    if not MODE_SIMULATION_ACTIF:
        print("[*] 📡 Connexion aux infrastructures Falcon pour calage de l'horloge maître...")
        try:
            url_horloge_api = "https://online.macsa.com.tn:8443/marketApi/sbe/market-report/11,12,13,32,33,99"
            res = requests.get(url_horloge_api, headers={"User-Agent": "Mozilla/5.0"}, timeout=1.5, verify=False)
            if res.status_code == 200:
                texte_nettoye = str(res.text).replace('"', '').replace("'", "").strip()
                regex_heure = re.findall(r'(?:\d|2[0-3]):[0-5]\d:[0-5]\d', texte_nettoye)
                if regex_heure and len(regex_heure) > 0: 
                    HEURE_CENTRALE_BVMT = str(regex_heure[0]).strip()
        except:
            pass
    else: 
        HEURE_CENTRALE_BVMT = "08:30:00"
    
    print("[*] 📡 Signal d'éveil SBE initialisé.\n[*] 📡 Métronome configuré en mode TURBO CONTINU (20 ms).\n")
    
    # Alignement d'amorce à froid
    DATE_JOUR_STR = date_instant_pc
    DATE_SEANCE_EN_COURS = date_instant_pc
    REGIME_ACTIF = detecter_regime_horaire(datetime.date.today())
    
    PREFIXE_LOG = "bourse_historique_reel_" if MODE_PRODUCTION else "bourse_historique_simulation_"
    FICHIER_HISTO_RAMDISK = os.path.join(REPERTOIRE_JSON, f"temp_{PREFIXE_LOG}{DATE_JOUR_STR}.csv")
    FICHIER_HISTO_SEANCE = os.path.join(REPERTOIRE_HISTO_REEL, f"{PREFIXE_LOG}{DATE_JOUR_STR}.csv")
    
    if MODE_PRODUCTION:
        FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_reel_{DATE_JOUR_STR}.csv")
        FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_reel_{DATE_JOUR_STR}.json")
    else:
        FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_simulation_{DATE_JOUR_STR}.csv")
        FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_simulation_{DATE_JOUR_STR}.json")

    if not os.path.exists(FICHIER_HISTO_RAMDISK) and (est_jour_ouvre_demarrage or not MODE_PRODUCTION):
        executer_purge_et_reinitialisation_seance_a_chaud(date_instant_pc)

    # Boucle souveraine de sûreté cadencée
    while True:
        try: 
            maintenant_obj = datetime.datetime.now()
            heure_courante_pc = maintenant_obj.strftime("%H:%M:%S")
            date_instant_pc = maintenant_obj.strftime("%d%m%Y")
            est_jour_ouvre_courant = maintenant_obj.weekday() < 5
            
            # Surveillance continue après 08:01:00 pour purger tout fichier de la veille résiduel
            if heure_courante_pc > "08:01:00":
                verifier_et_nettoyer_fichiers_veille(date_instant_pc)
            
            # Détection de bascule de séance (changement de jour après 08:00:00)
            if heure_courante_pc >= "08:00:00" and date_instant_pc != DATE_SEANCE_EN_COURS:
                print(f"\n[🛡️ SEUIL DE SÉANCE] Mutation horaire détectée ({DATE_SEANCE_EN_COURS} -> {date_instant_pc}).")
                
                # 1. Sauvegarde résiduelle de la veille si présente
                if os.path.exists(FICHIER_HISTO_RAMDISK) and os.path.getsize(FICHIER_HISTO_RAMDISK) > 100:
                    try:
                        df_backup = pd.read_csv(FICHIER_HISTO_RAMDISK, sep=";", low_memory=False).dropna(how='all')
                        if not df_backup.empty and MODE_PRODUCTION:
                            df_backup.to_csv(FICHIER_HISTO_SEANCE, sep=';', index=False, encoding='utf-8', lineterminator="\n")
                            print("[🏁 CONSOLIDATION] Grand livre de la veille sCELLé avec succès avant purge.")
                    except Exception as e_back:
                        print(f"⚠️ Échec sauvegarde résiduelle bascule : {e_back}")

                # 2. Réinitialisation des structures RAM à chaud
                executer_purge_et_reinitialisation_seance_a_chaud(date_instant_pc)
                HEURE_CENTRALE_BVMT = "08:30:00"
                
                # 3. Re-routage dynamique des chemins de fichiers pour la nouvelle journée
                DATE_JOUR_STR = date_instant_pc
                PREFIXE_LOG = "bourse_historique_reel_" if MODE_PRODUCTION else "bourse_historique_simulation_"
                
                FICHIER_HISTO_RAMDISK = os.path.join(REPERTOIRE_JSON, f"temp_{PREFIXE_LOG}{DATE_JOUR_STR}.csv")
                FICHIER_HISTO_SEANCE = os.path.join(REPERTOIRE_HISTO_REEL, f"{PREFIXE_LOG}{DATE_JOUR_STR}.csv")
                
                if MODE_PRODUCTION:
                    FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_reel_{DATE_JOUR_STR}.csv")
                    FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_reel_{DATE_JOUR_STR}.json")
                else:
                    FICHIER_SYNTHESE_CLOTURE = os.path.join(REPERTOIRE_SYNTHESE, f"synthese_cloture_simulation_{DATE_JOUR_STR}.csv")
                    FICHIER_ETAT_RAM = os.path.join(REPERTOIRE_JSON, f"bourse_state_RAM_simulation_{DATE_JOUR_STR}.json")
                
                DATE_SEANCE_EN_COURS = date_instant_pc
                REGIME_ACTIF = detecter_regime_horaire(datetime.date.today())
                print(f"[✅ SUCCÈS] Tuyauterie Cloud ré-alignée sur les nouveaux fichiers du {DATE_JOUR_STR}.\n")
            
            # Exécution du moteur d'analyse principal
            recuperer_et_analyser()
            if MODE_PRODUCTION and os.path.exists(FICHIER_HISTO_RAMDISK) and (time.time() - getattr(lancer_collecteur_bvmt, "_last_hist_sync", 0) >= 2.0):
                lancer_collecteur_bvmt._last_hist_sync = time.time()
                try:
                    if not os.path.exists(FICHIER_HISTO_SEANCE) or os.path.getsize(FICHIER_HISTO_RAMDISK) != os.path.getsize(FICHIER_HISTO_SEANCE):
                        os.makedirs(os.path.dirname(FICHIER_HISTO_SEANCE), exist_ok=True)
                        tmp_dest = FICHIER_HISTO_SEANCE + ".tmp"
                        shutil.copy2(FICHIER_HISTO_RAMDISK, tmp_dest)
                        os.replace(tmp_dest, FICHIER_HISTO_SEANCE)
                except Exception:
                    pass
            
            # Affichage console synchronisé
            if not est_jour_ouvre_courant and MODE_PRODUCTION:
                str_label_mode = "WEEK-END (VEILLE)"
            else:
                str_label_mode = "PRODUCTION_REEL" if MODE_PRODUCTION else "SIMULATION_VIRTUELLE"
                
            trame_heartbeat = f"[*] [🦅 NOYAU MASTER] PC: {heure_courante_pc} | BVMT: {HEURE_CENTRALE_BVMT} | Mode: {str_label_mode} | Noyau RAM: OK"
            sys.stdout.write(f"\r{trame_heartbeat.ljust(110)[:110]}\x1b[K")
            sys.stdout.flush()
            
        except Exception as e_boucle:
            sys.stdout.write(f"\r⚠️ Alerte boucle souveraine : {e_boucle}")
            sys.stdout.flush()
            
        # Temporisation de sûreté
        time.sleep(0.02)





import traceback
import spaces, gradio as gr

@spaces.GPU
def _hf_gpu_ping():
    return "OK"

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_HIST_DIR = os.path.join(_BASE_DIR, "Historique")
_CACHE_DIR = os.path.join(_BASE_DIR, "Cache_Live")
_INDEX_FILE = os.path.join(_BASE_DIR, "index.json")
os.makedirs(_HIST_DIR, exist_ok=True)
os.makedirs(_CACHE_DIR, exist_ok=True)

_DIAG_PY = []
def log_diag_py(tag, msg):
    h_tunis = datetime.datetime.now().strftime("%H:%M:%S")
    entry = f"[{h_tunis}] [{tag}] {msg}"
    _DIAG_PY.append(entry)
    if len(_DIAG_PY) > 50:
        _DIAG_PY.pop(0)

def _generer_index_et_whitelist():
    hf = []
    fichiers_autorises = [os.path.join(_BASE_DIR, "CH-WaveCandle.html"), _INDEX_FILE]
    for root, _, files in os.walk(_HIST_DIR):
        if "Backup" in root: continue
        for fn in files:
            if fn.endswith(".csv"):
                full_p = os.path.join(root, fn)
                fichiers_autorises.append(full_p)
                hf.append(os.path.relpath(full_p, _HIST_DIR).replace("\\", "/"))
    lf = []
    if os.path.exists(_CACHE_DIR):
        for fn in os.listdir(_CACHE_DIR):
            fp = os.path.join(_CACHE_DIR, fn)
            if os.path.isfile(fp) and fn != "index.json":
                fichiers_autorises.append(fp)
                lf.append({"Name": fn, "Length": os.path.getsize(fp), "Mtime": int(os.path.getmtime(fp) * 1000)})
    payload = {
        "base": _BASE_DIR.replace("\\", "/"),
        "hist": sorted(hf),
        "live": lf,
        "py": True,
        "url": globals().get("URL_SBE", "https://macsa.com.tn"),
        "err": _DIAG_PY[-1] if _DIAG_PY else "",
        "diag": list(_DIAG_PY),
        "gh_ok": bool(GH_TOKEN and GH_REPO)
    }
    with open(_INDEX_FILE, "w", encoding="utf-8") as f:
        f.write(json.dumps(payload))
    gr.set_static_paths(paths=fichiers_autorises)

# Exécuter avant le lancement pour que index.json et tous les CSV soient whitelistes dans Gradio
_generer_index_et_whitelist()

def _boucle_index_cloud():
    while True:
        try:
            _generer_index_et_whitelist()
        except Exception as ex:
            log_diag_py("INDEX_LOOP_ERR", str(ex))
        time.sleep(1.5)

import hashlib, random, smtplib
from email.message import EmailMessage
import hashlib, random, smtplib
from email.message import EmailMessage

_USERS_FILE = os.path.join(_BASE_DIR, "users_db.json")
_TOKENS_ACTIFS = {}
_PENDING_OTP = {}

import urllib.request, urllib.error
def _get_users_repo():
    return os.environ.get("GH_USERS_REPO", "").strip() or globals().get("GH_REPO", "").strip()

def _pull_users_github():
    repo = _get_users_repo()
    tk = globals().get("GH_TOKEN", "").strip()
    if not tk or not repo: return
    api_url = f"https://api.github.com/repos/{repo}/contents/users_db.json"
    headers = {
        "Authorization": f"Bearer {tk}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "CH-WaveCandle-HFT"
    }
    try:
        req = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(req, timeout=12) as resp:
            info = json.loads(resp.read().decode("utf-8"))
            raw_content = base64.b64decode(info["content"]).decode("utf-8")
            with open(_USERS_FILE, "w", encoding="utf-8") as f:
                f.write(raw_content)
    except urllib.error.HTTPError as he:
        if he.code != 404:
            log_diag_py("GH_PULL_USERS_HTTP", f"HTTP {he.code}")
    except Exception as ex:
        log_diag_py("GH_PULL_USERS_ERR", str(ex))

def _push_users_github():
    repo = _get_users_repo()
    tk = globals().get("GH_TOKEN", "").strip()
    if not tk or not repo or not os.path.exists(_USERS_FILE): return
    api_url = f"https://api.github.com/repos/{repo}/contents/users_db.json"
    headers = {
        "Authorization": f"Bearer {tk}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "CH-WaveCandle-HFT",
        "Content-Type": "application/json"
    }
    try:
        sha = None
        try:
            req_get = urllib.request.Request(api_url, headers=headers)
            with urllib.request.urlopen(req_get, timeout=10) as r_get:
                sha = json.loads(r_get.read().decode("utf-8")).get("sha")
        except urllib.error.HTTPError as he:
            if he.code != 404: raise
        with open(_USERS_FILE, "rb") as f:
            b64_data = base64.b64encode(f.read()).decode("ascii")
        payload = {"message": "Sync users_db.json", "content": b64_data}
        if sha: payload["sha"] = sha
        req_put = urllib.request.Request(api_url, data=json.dumps(payload).encode("utf-8"), headers=headers, method="PUT")
        urllib.request.urlopen(req_put, timeout=12).read()
    except Exception as ex:
        log_diag_py("GH_PUSH_USERS_ERR", str(ex))

def _charger_users():
    if not os.path.exists(_USERS_FILE):
        _pull_users_github()
    if os.path.exists(_USERS_FILE):
        try:
            with open(_USERS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"users": {}}

def _sauver_users(db_obj):
    with open(_USERS_FILE, "w", encoding="utf-8") as f:
        json.dump(db_obj, f, indent=2, ensure_ascii=False)
    threading.Thread(target=_push_users_github, daemon=True).start()

def _hash_pwd(email: str, pwd: str):
    salt = os.urandom(16).hex()
    dk = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt.encode("utf-8"), 100000).hex()
    return f"pbkdf2_sha256${salt}${dk}"

def _verifier_pwd(email: str, pwd: str, stored_hash: str):
    if not stored_hash:
        return False
    if stored_hash.startswith("pbkdf2_sha256$"):
        parts = stored_hash.split("$")
        if len(parts) != 3:
            return False
        _, salt, expected_dk = parts
        dk = hashlib.pbkdf2_hmac("sha256", pwd.encode("utf-8"), salt.encode("utf-8"), 100000).hex()
        return (lambda a, b: len(a) == len(b) and sum(ord(x) ^ ord(y) for x, y in zip(a, b)) == 0)(dk, expected_dk)
    # Rétrocompatibilité avec les anciens comptes SHA-256
    legacy = hashlib.sha256(f"{email.lower().strip()}:{pwd}:BVMT_SALT_2026".encode("utf-8")).hexdigest()
    return (lambda a, b: len(a) == len(b) and sum(ord(x) ^ ord(y) for x, y in zip(a, b)) == 0)(legacy, stored_hash)

def _envoyer_code_mail(dest_email: str, code_otp: str):
    sujet = f"[{code_otp}] Code de validation CH-WaveCandle HFT"
    corps = f"Bienvenue sur la station CH-WaveCandle HFT (Bourse de Tunis).\n\nVotre code de validation à 6 chiffres est : {code_otp}\n\nCe code est valable 15 minutes."
    webhook = os.environ.get("MAIL_WEBHOOK_URL", "").strip()
    if webhook:
        try:
            req = urllib.request.Request(
                webhook,
                data=json.dumps({"to": dest_email, "subject": sujet, "body": corps}).encode("utf-8"),
                headers={"Content-Type": "application/json", "User-Agent": "CH-WaveCandle-HFT"},
                method="POST"
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                rep_txt = resp.read().decode("utf-8", errors="replace")
                if resp.status in (200, 201, 202, 302) and "ServiceLogin" not in rep_txt:
                    return True, ""
                return False, "Webhook Google Apps Script non autorisé (vérifiez accès : Tout le monde)."
        except Exception as ex:
            return False, f"Webhook Mail : {ex}"

    api_key = os.environ.get("RESEND_API_KEY", "").strip()
    if not api_key:
        return False, "Configurez MAIL_WEBHOOK_URL (Google Apps Script) ou RESEND_API_KEY."
    expediteur = os.environ.get("RESEND_FROM", "CH-WaveCandle BVMT <onboarding@resend.dev>").strip()
    try:
        payload = {"from": expediteur, "to": [dest_email], "subject": sujet, "text": corps}
        req = urllib.request.Request(
            "https://resend.com",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json", "User-Agent": "CH-WaveCandle-HFT"},
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=12) as resp:
            return (True, "") if resp.status in (200, 201, 202) else (False, f"HTTP {resp.status}")
    except urllib.error.HTTPError as he:
        return False, f"HTTP {he.code} : {he.read().decode('utf-8', errors='replace')}"
    except Exception as ex:
        return False, str(ex)
def _rpc_register(payload_json: str):
    try:
        b = json.loads(payload_json)
        nom = str(b.get("nom", "")).strip()
        email = str(b.get("email", "")).strip().lower()
        pwd = str(b.get("pass", ""))
        if "@" not in email or "." not in email.split("@")[-1]:
            return json.dumps({"ok": False, "err": "Adresse e-mail invalide."})
        if len(pwd) < 8 or not any(c.isdigit() for c in pwd) or not any(c.isalpha() for c in pwd):
            return json.dumps({"ok": False, "err": "Mot de passe : 8 caractères min. avec lettres et chiffres."})
        db = _charger_users()
        if email in db and db[email].get("verified"):
            return json.dumps({"ok": False, "err": "Cet e-mail est déjà inscrit. Connectez-vous."})
        otp = f"{random.randint(100000, 999999)}"
        _PENDING_OTP[email] = {
            "otp": otp, "exp": time.time() + 900, "nom": nom,
            "hash": _hash_pwd(email, pwd),
            "date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        }
        ok_mail, err_mail = _envoyer_code_mail(email, otp)
        if not ok_mail:
            return json.dumps({"ok": False, "err": f"Échec envoi e-mail : {err_mail}"})
        return json.dumps({"ok": True, "msg": f"Code envoyé à {email}"})
    except Exception as ex:
        return json.dumps({"ok": False, "err": str(ex)})

def _rpc_verify(payload_json: str):
    try:
        b = json.loads(payload_json)
        email = str(b.get("email", "")).strip().lower()
        code_saisi = str(b.get("code", "")).strip()
        pend = _PENDING_OTP.get(email)
        if not pend or time.time() > pend["exp"]:
            return json.dumps({"ok": False, "err": "Code expiré ou inexistant. Recommencez."})
        if code_saisi != pend["otp"]:
            return json.dumps({"ok": False, "err": "Code à 6 chiffres incorrect."})
        db = _charger_users()
        db[email] = {
            "nom": pend["nom"], "hash": pend["hash"],
            "verified": True, "plan": "ACTIF", "inscrit_le": pend["date"]
        }
        _sauver_users(db)
        del _PENDING_OTP[email]
        tk = hashlib.sha256(f"{email}:{time.time()}:CH_SESSION".encode("utf-8")).hexdigest()[:32]
        _TOKENS_ACTIFS[tk] = email
        return json.dumps({"ok": True, "token": tk, "user": pend["nom"] or email})
    except Exception as ex:
        return json.dumps({"ok": False, "err": str(ex)})

def _rpc_login(payload_json: str):
    try:
        b = json.loads(payload_json)
        email = str(b.get("email", "")).strip().lower()
        pwd = str(b.get("pass", ""))
        db = _charger_users()
        u = db.get(email)
        if u and u.get("verified") and _verifier_pwd(email, pwd, u.get("hash", "")):
            tk = hashlib.sha256(f"{email}:{time.time()}:CH_SESSION".encode("utf-8")).hexdigest()[:32]
            _TOKENS_ACTIFS[tk] = email
            return json.dumps({"ok": True, "token": tk, "user": u.get("nom") or email})
        return json.dumps({"ok": False, "err": "E-mail ou mot de passe incorrect."})
    except Exception as ex:
        return json.dumps({"ok": False, "err": str(ex)})

def _get_live_dirs():
    dirs = []
    for d in [globals().get("REPERTOIRE_JSON"), "/dev/shm/Bourse_MACSA_Cache", globals().get("_CACHE_DIR"), os.path.join(tempfile.gettempdir(), "Bourse_MACSA_Cache")]:
        if d and os.path.exists(d) and d not in dirs:
            dirs.append(d)
    return dirs

def _construire_index():
    hf, hmeta = [], {}
    for root, _, files in os.walk(_HIST_DIR):
        if "Backup" in root: continue
        for fn in files:
            if fn.endswith(".csv"):
                fp = os.path.join(root, fn)
                rel = os.path.relpath(fp, _HIST_DIR).replace("\\", "/")
                hf.append(rel)
                hmeta[rel] = {"size": os.path.getsize(fp), "mtime": int(os.path.getmtime(fp) * 1000)}
    lf = []
    vus = set()
    date_jour = datetime.datetime.now().strftime("%d%m%Y")
    for live_dir in _get_live_dirs():
        for fn in os.listdir(live_dir):
            fp = os.path.join(live_dir, fn)
            if not os.path.isfile(fp) or fn == "index.json" or fn in vus:
                continue
            if fn.startswith("temp_bourse_historique_") and date_jour not in fn:
                try: os.remove(fp)
                except Exception: pass
                continue
            vus.add(fn)
            lf.append({
                "Name": fn,
                "Length": os.path.getsize(fp),
                "Mtime": int(os.path.getmtime(fp) * 1000)
            })
    lf.sort(key=lambda x: x["Mtime"], reverse=True)
    return {
        "hist": sorted(hf),
        "hist_meta": hmeta,
        "live": lf,
        "server_date": date_jour,
        "py": True,
        "url": globals().get("URL_SBE", "https://macsa.com.tn"),
        "err": ""
    }

_MULTI_AGG_CACHE = {}

def _calculer_multi_rapide(ticker_cible: str):
    tk_up = ticker_cible.strip().upper()
    bougies = []
    for root, _, files in os.walk(_HIST_DIR):
        if "Backup" in root: continue
        for fn in sorted(files):
            if not fn.endswith(".csv"): continue
            fp = os.path.join(root, fn)
            mt = int(os.path.getmtime(fp))
            key = (fp, mt)
            if key not in _MULTI_AGG_CACHE:
                par_tk = {}
                try:
                    with open(fp, "r", encoding="utf-8-sig", errors="replace") as f:
                        header = f.readline().strip().split(";")
                        idx_tk = next((i for i, c in enumerate(header) if "VALEUR" in c.upper() or "TICKER" in c.upper()), 0)
                        idx_px = next((i for i, c in enumerate(header) if "DERNIER" in c.upper() or "PRIX" in c.upper()), 4)
                        idx_qt = next((i for i, c in enumerate(header) if "QUANTITE" in c.upper() or "QTE" in c.upper()), 6)
                        idx_sn = next((i for i, c in enumerate(header) if "SENS" in c.upper()), 10)
                        for line in f:
                            cols = line.split(";")
                            if len(cols) <= max(idx_tk, idx_px, idx_qt): continue
                            v_tk = cols[idx_tk].strip().strip('"').upper()
                            if not v_tk: continue
                            try:
                                px = float(cols[idx_px].replace(",", ".").strip().strip('"'))
                                qt = int(float(cols[idx_qt].replace(" ", "").replace(",", ".").strip().strip('"')))
                            except Exception:
                                continue
                            if px <= 0 or qt <= 0: continue
                            sn = cols[idx_sn].strip().strip('"').upper() if idx_sn < len(cols) else ""
                            st = par_tk.get(v_tk)
                            if not st:
                                st = {"o": px, "h": px, "l": px, "c": px, "v": 0, "m": 0.0, "qa": 0, "ma": 0.0, "qv": 0, "mv": 0.0, "qn": 0, "mn": 0.0, "n": 0}
                                par_tk[v_tk] = st
                            if px > st["h"]: st["h"] = px
                            if px < st["l"]: st["l"] = px
                            st["c"] = px
                            st["v"] += qt
                            st["m"] += px * qt
                            st["n"] += 1
                            if "ACH" in sn:
                                st["qa"] += qt; st["ma"] += px * qt
                            elif "VEN" in sn:
                                st["qv"] += qt; st["mv"] += px * qt
                            else:
                                st["qn"] += qt; st["mn"] += px * qt
                except Exception:
                    pass
                _MULTI_AGG_CACHE[key] = par_tk
            if tk_up in _MULTI_AGG_CACHE[key]:
                st = _MULTI_AGG_CACHE[key][tk_up]
                d_str = "".join(ch for ch in fn if ch.isdigit())
                date_fr = f"{d_str[:2]}/{d_str[2:4]}/{d_str[4:8]}" if len(d_str) >= 8 else fn
                bougies.append({"date": date_fr, **st})
    return bougies

_CHAT_FILE = os.path.join(_BASE_DIR, "chat_messages.json")
_CHAT_MEDIA_DIR = os.path.join(_BASE_DIR, "chat_media")
os.makedirs(_CHAT_MEDIA_DIR, exist_ok=True)
_CHAT_LOCK = threading.Lock()
_CHAT_BOOT_SYNCED = False

def _hf_data_cfg():
    tk = (os.environ.get("HF_TOKEN", "") or os.environ.get("HUGGINGFACEHUB_API_TOKEN", "")).strip()
    repo = (os.environ.get("HF_DATA_REPO", "") or "HafDev/ch-wavecandle-data").strip()
    return tk, repo

def _sync_chat_depuis_github():
    global _CHAT_BOOT_SYNCED, _LAST_GH_SYNC_STATUS
    if _CHAT_BOOT_SYNCED:
        return
    _CHAT_BOOT_SYNCED = True
    tk, repo = _hf_data_cfg()
    if not tk or not repo:
        _LAST_GH_SYNC_STATUS = "HF_TOKEN manquant"
        return
    try:
        from huggingface_hub import hf_hub_download
        dl_path = hf_hub_download(repo_id=repo, filename="chat_messages.json", repo_type="dataset", token=tk)
        with open(dl_path, "r", encoding="utf-8") as rf:
            rem_msgs = json.load(rf)
        if isinstance(rem_msgs, list):
            loc_msgs = []
            if os.path.isfile(_CHAT_FILE):
                with open(_CHAT_FILE, "r", encoding="utf-8") as lf:
                    loc_msgs = json.load(lf) or []
            fusion = {str(m.get("id")): m for m in (rem_msgs + loc_msgs) if isinstance(m, dict) and m.get("id")}
            merged = sorted(fusion.values(), key=lambda x: str(x.get("id", "")))[-500:]
            with open(_CHAT_FILE, "w", encoding="utf-8") as wf:
                json.dump(merged, wf, ensure_ascii=False, indent=2)
        _LAST_GH_SYNC_STATUS = "OK"
    except Exception as e:
        _LAST_GH_SYNC_STATUS = f"Init ({type(e).__name__})"

def _push_fichier_gh(rel_gh_path: str, local_fp: str, commit_msg: str):
    def _worker():
        global _LAST_GH_SYNC_STATUS
        try:
            tk, repo = _hf_data_cfg()
            if not tk or not repo or not os.path.isfile(local_fp):
                _LAST_GH_SYNC_STATUS = "HF_TOKEN manquant"
                return
            from huggingface_hub import HfApi
            api = HfApi(token=tk)
            api.upload_file(
                path_or_fileobj=local_fp,
                path_in_repo=rel_gh_path,
                repo_id=repo,
                repo_type="dataset",
                commit_message=commit_msg
            )
            _LAST_GH_SYNC_STATUS = "OK"
        except Exception as ex_hf:
            _LAST_GH_SYNC_STATUS = f"ERR: {ex_hf}"
            print(f"[!] Erreur Sync HF Dataset ({rel_gh_path}):", ex_hf)
    threading.Thread(target=_worker, daemon=True).start()

_PG_READY = False
_PG_INIT_DONE = False
_CHAT_RAM_CACHE = {}

def _get_pg_conn():
    db_url = os.environ.get("DATABASE_URL", "").strip()
    if not db_url:
        return None
    import psycopg2
    return psycopg2.connect(db_url, connect_timeout=6)

def _init_pg_tables():
    global _PG_READY, _PG_INIT_DONE
    if _PG_INIT_DONE:
        return _PG_READY
    _PG_INIT_DONE = True
    try:
        conn = _get_pg_conn()
        if not conn:
            return False
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ch_users (
                        email TEXT PRIMARY KEY,
                        data_json TEXT NOT NULL,
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS ch_messages (
                        id TEXT PRIMARY KEY,
                        salon TEXT NOT NULL,
                        data_json TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ch_messages_salon_id ON ch_messages (salon, id);")
        conn.close()
        _PG_READY = True
        print("[OK] Base PostgreSQL Neon connectée et tables ch_users / ch_messages prêtes !")
    except Exception as e:
        print("[!] PostgreSQL Neon indisponible (repli fichier/HF actif):", e)
        _PG_READY = False
    return _PG_READY

def _pg_charger_users():
    if not _init_pg_tables():
        return None
    try:
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT email, data_json FROM ch_users;")
            rows = cur.fetchall()
        conn.close()
        out = {}
        for r in rows:
            rl = list(r)
            out[rl.pop(0)] = json.loads(rl.pop(0))
        return out
    except Exception as e:
        print("[!] Erreur lecture PG users:", e)
        return None

def _pg_sauver_users(db_dict):
    if not _init_pg_tables():
        return False
    try:
        conn = _get_pg_conn()
        with conn:
            with conn.cursor() as cur:
                for em, info in (db_dict or {}).items():
                    cur.execute("""
                        INSERT INTO ch_users (email, data_json, updated_at)
                        VALUES (%s, %s, NOW())
                        ON CONFLICT (email) DO UPDATE SET data_json = EXCLUDED.data_json, updated_at = NOW();
                    """, (str(em).lower().strip(), json.dumps(info, ensure_ascii=False)))
        conn.close()
        return True
    except Exception as e:
        print("[!] Erreur écriture PG users:", e)
        return False

def _pg_charger_messages():
    if not _init_pg_tables():
        return None
    try:
        conn = _get_pg_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT data_json FROM ch_messages ORDER BY id ASC LIMIT 1000;")
            rows = cur.fetchall()
        conn.close()
        return [json.loads(list(r).pop(0)) for r in rows]
    except Exception as e:
        print("[!] Erreur lecture PG messages:", e)
        return None

def _pg_sauver_messages(msgs):
    if not _init_pg_tables():
        return False
    try:
        conn = _get_pg_conn()
        ids_actuels = [str(m.get("id")) for m in msgs if m.get("id")]
        with conn:
            with conn.cursor() as cur:
                if ids_actuels:
                    cur.execute("DELETE FROM ch_messages WHERE NOT (id = ANY(%s));", (ids_actuels,))
                else:
                    cur.execute("DELETE FROM ch_messages;")
                for m in msgs[-1000:]:
                    mid = str(m.get("id") or "")
                    if not mid:
                        continue
                    salon = str(m.get("salon") or "GENERAL").upper()
                    cur.execute("""
                        INSERT INTO ch_messages (id, salon, data_json)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET salon = EXCLUDED.salon, data_json = EXCLUDED.data_json;
                    """, (mid, salon, json.dumps(m, ensure_ascii=False)))
        conn.close()
        _CHAT_RAM_CACHE.clear()
        return True
    except Exception as e:
        print("[!] Erreur écriture PG messages:", e)
        return False

def _charger_messages_chat():
    pg_msgs = _pg_charger_messages()
    if pg_msgs is not None:
        if len(pg_msgs) > 0:
            return pg_msgs
        # Si PostgreSQL est vide au 1er démarrage, migrer l'historique existant depuis HF Dataset
        _sync_chat_depuis_github()
        if os.path.isfile(_CHAT_FILE):
            try:
                with open(_CHAT_FILE, "r", encoding="utf-8") as f:
                    seed = json.load(f) or []
                if seed:
                    _pg_sauver_messages(seed)
                    return seed
            except Exception:
                pass
        return []
    _sync_chat_depuis_github()
    try:
        if os.path.isfile(_CHAT_FILE):
            with open(_CHAT_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data[-500:]
    except Exception:
        pass
    return []

_CHAT_SYNC_TIMER = None
_CHAT_SYNC_LOCK = threading.Lock()
_ADMIN_EMAILS = {"chemshaf@gmail.com"}

def _sauver_messages_chat(msgs):
    global _CHAT_SYNC_TIMER
    _pg_sauver_messages(msgs)
    try:
        tmp = _CHAT_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(msgs[-1000:], f, ensure_ascii=False, indent=2)
        os.replace(tmp, _CHAT_FILE)
    except Exception:
        pass
    # Regrouper les commits HF Hub toutes les 12 secondes pour éviter tout rate-limit API
    with _CHAT_SYNC_LOCK:
        if _CHAT_SYNC_TIMER is not None:
            try:
                _CHAT_SYNC_TIMER.cancel()
            except Exception:
                pass
        _CHAT_SYNC_TIMER = threading.Timer(12.0, lambda: _push_fichier_gh("chat_messages.json", _CHAT_FILE, "Sync groupée forum communautaire"))
        _CHAT_SYNC_TIMER.daemon = True
        _CHAT_SYNC_TIMER.start()

def _enregistrer_media_direct(raw_url, orig_name=""):
    try:
        s = str(raw_url or "").strip()
        if not s or len(s) > 2_800_000:
            return ""
        header = s[:80].lower()
        b64_part = s.split(",", 1).pop() if "," in s else s
        if not b64_part:
            return ""
        if "pdf" in header or orig_name.lower().endswith(".pdf"):
            ext = "pdf"
        elif "csv" in header or orig_name.lower().endswith(".csv"):
            ext = "csv"
        elif "spreadsheet" in header or "excel" in header or orig_name.lower().endswith(".xlsx"):
            ext = "xlsx"
        elif "jpeg" in header or "jpg" in header:
            ext = "jpg"
        else:
            ext = "png"
        mid = f"media_{int(time.time() * 1000)}_{os.urandom(3).hex()}.{ext}"
        fp = os.path.join(_CHAT_MEDIA_DIR, mid)
        with open(fp, "wb") as f:
            f.write(base64.b64decode(b64_part))
        _push_fichier_gh(f"chat_media/{mid}", fp, f"Media forum {mid}")
        return mid
    except Exception as e:
        print("[!] Erreur enregistrement media chat:", e)
        return ""

def _email_depuis_token(tk_val):
    u = _TOKENS_ACTIFS.get(tk_val)
    if isinstance(u, dict):
        return str(u.get("email") or "").strip().lower()
    if isinstance(u, str):
        return u.strip().lower()
    return ""

def _lire_xlsx_table(fp_xlsx, max_rows=300):
    import zipfile, xml.etree.ElementTree as ET
    rows_out = []
    with zipfile.ZipFile(fp_xlsx, "r") as zf:
        shared = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root_ss = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root_ss.iter():
                if si.tag.endswith("}si"):
                    txt_parts = [t.text or "" for t in si.iter() if t.tag.endswith("}t")]
                    shared.append("".join(txt_parts))
        sheets = sorted([n for n in zf.namelist() if n.startswith("xl/worksheets/sheet") and n.endswith(".xml")])
        if not sheets:
            return []
        root_sh = ET.fromstring(zf.read(sheets.pop(0)))
        for row_el in root_sh.iter():
            if row_el.tag.endswith("}row"):
                r_vals = []
                for c_el in row_el:
                    if c_el.tag.endswith("}c"):
                        ctype = c_el.attrib.get("t", "")
                        val_str = ""
                        for child in c_el:
                            if child.tag.endswith("}v") and child.text is not None:
                                if ctype == "s":
                                    idx_s = int(child.text)
                                    val_str = shared[idx_s] if 0 <= idx_s < len(shared) else child.text
                                else:
                                    val_str = child.text
                            elif child.tag.endswith("}is"):
                                val_str = "".join([t.text or "" for t in child.iter() if t.tag.endswith("}t")])
                        r_vals.append(val_str)
                if any(v != "" for v in r_vals):
                    rows_out.append(r_vals)
                if len(rows_out) >= max_rows:
                    break
    return rows_out

def _extraire_mini_apercu(mid):
    try:
        if not mid:
            return None
        fp = os.path.join(_CHAT_MEDIA_DIR, mid)
        if not os.path.isfile(fp):
            tk_g, repo_g = _hf_data_cfg()
            if tk_g and repo_g:
                from huggingface_hub import hf_hub_download
                dl = hf_hub_download(repo_id=repo_g, filename=f"chat_media/{mid}", repo_type="dataset", token=tk_g)
                shutil.copyfile(dl, fp)
        if not os.path.isfile(fp):
            return None
        if mid.endswith(".xlsx"):
            rows = _lire_xlsx_table(fp, max_rows=5)
            return [r[:5] for r in rows[:4]]
        if mid.endswith(".csv"):
            with open(fp, "r", encoding="utf-8-sig", errors="replace") as f:
                raw_lines = [ln.strip() for ln in f.read().splitlines() if ln.strip()][:4]
            if not raw_lines:
                return None
            first_ln = raw_lines.copy().pop(0)
            sep = ";" if ";" in first_ln else ("\t" if "\t" in first_ln else ",")
            return [[c.strip().strip('"') for c in ln.split(sep)[:5]] for ln in raw_lines]
        if mid.endswith(".pdf"):
            try:
                import fitz
                doc = fitz.open(fp)
                txt = doc.load_page(0).get_text("text").strip() if len(doc) > 0 else ""
                lines = [ln.strip() for ln in txt.splitlines() if ln.strip()][:5]
                return [[f"Page 1 / {len(doc)}"]] + [[ln[:90]] for ln in lines] if lines else [[f"Document PDF ({len(doc)} page(s))"]]
            except Exception:
                with open(fp, "rb") as fb:
                    raw_b = fb.read(120000)
                nb_pages = raw_b.count(b"/Type /Page") - raw_b.count(b"/Type /Pages")
                return [[f"Document PDF ({max(1, nb_pages)} page(s))"], ["Cliquez pour ouvrir ou télécharger le document"]]
    except Exception as e:
        print("[!] Erreur mini apercu:", e)
    return None

def _generer_miniature_pdf_b64(mid):
    try:
        if not mid or not mid.endswith(".pdf"):
            return ""
        fp_pdf = os.path.join(_CHAT_MEDIA_DIR, mid)
        fp_png = os.path.join(_CHAT_MEDIA_DIR, f"thumb_{mid}.png")
        if not os.path.isfile(fp_png):
            if not os.path.isfile(fp_pdf):
                tk_g, repo_g = _hf_data_cfg()
                if tk_g and repo_g:
                    from huggingface_hub import hf_hub_download
                    dl = hf_hub_download(repo_id=repo_g, filename=f"chat_media/{mid}", repo_type="dataset", token=tk_g)
                    shutil.copyfile(dl, fp_pdf)
            if os.path.isfile(fp_pdf):
                import fitz
                doc = fitz.open(fp_pdf)
                if len(doc) > 0:
                    pix = doc.load_page(0).get_pixmap(matrix=fitz.Matrix(0.75, 0.75), alpha=False)
                    pix.save(fp_png)
        if os.path.isfile(fp_png):
            with open(fp_png, "rb") as f:
                return "data:image/png;base64," + base64.b64encode(f.read()).decode("ascii")
    except Exception as e:
        print("[!] Erreur miniature PDF:", e)
    return ""

def _rpc_data(payload_json: str):
    try:
        b = json.loads(payload_json)
        tk = str(b.get("token", ""))
        if tk not in _TOKENS_ACTIFS:
            return json.dumps({"status": 401, "err": "Authentification requise"})
        cmd = str(b.get("cmd", ""))
        if cmd == "multi_agg":
            return json.dumps({"status": 200, "bougies": _calculer_multi_rapide(str(b.get("ticker", "")))})
        if cmd == "chat_upload_media":
            b64_data = str(b.get("data_url", "")).strip()
            if not b64_data or len(b64_data) > 2_800_000:
                return json.dumps({"status": 400, "err": "Image trop volumineuse (max 2 Mo)"})
            if "," in b64_data:
                b64_data = b64_data.split(",", 1).pop()[2]
            ext = "jpg" if "jpeg" in str(b.get("data_url", "")).lower() or "jpg" in str(b.get("data_url", "")).lower() else "png"
            media_id = f"media_{int(time.time() * 1000)}_{os.urandom(3).hex()}.{ext}"
            fp_m = os.path.join(_CHAT_MEDIA_DIR, media_id)
            with open(fp_m, "wb") as fm:
                fm.write(base64.b64decode(b64_data))
            _push_fichier_gh(f"chat_media/{media_id}", fp_m, f"Media forum {media_id}")
            return json.dumps({"status": 200, "media_id": media_id})

        if cmd == "chat_get_media":
            mid = os.path.basename(str(b.get("media_id", "")).strip())
            fp_m = os.path.join(_CHAT_MEDIA_DIR, mid)
            if not os.path.isfile(fp_m):
                tk_g, repo_g = _hf_data_cfg()
                if tk_g and repo_g and mid:
                    try:
                        from huggingface_hub import hf_hub_download
                        dl_m = hf_hub_download(repo_id=repo_g, filename=f"chat_media/{mid}", repo_type="dataset", token=tk_g)
                        shutil.copyfile(dl_m, fp_m)
                    except Exception:
                        pass
            if os.path.isfile(fp_m):
                mime = "application/pdf" if mid.endswith(".pdf") else ("text/csv" if mid.endswith(".csv") else ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if mid.endswith(".xlsx") else ("image/jpeg" if mid.endswith(".jpg") else "image/png")))
                with open(fp_m, "rb") as rf:
                    b64_out = base64.b64encode(rf.read()).decode("ascii")
                xlsx_rows = _lire_xlsx_table(fp_m) if mid.endswith(".xlsx") else None; return json.dumps({"status": 200, "data_url": f"data:{mime};base64,{b64_out}", "rows": xlsx_rows})
            return json.dumps({"status": 404, "err": "Média introuvable"})

        if cmd == "chat_edit":
            msg_id = str(b.get("id", "")).strip()
            nouv_txt = str(b.get("text", "")).strip()
            nouv_sent = str(b.get("sentiment", "")).strip().upper()
            req_email = _email_depuis_token(tk)
            if not msg_id or not nouv_txt or len(nouv_txt) > 1500:
                return json.dumps({"status": 400, "err": "Texte invalide"})
            with _CHAT_LOCK:
                msgs = _charger_messages_chat()
                trouve = False
                for m in msgs:
                    if str(m.get("id")) == msg_id:
                        owner = str(m.get("owner_email") or "").strip().lower()
                        if owner and req_email != owner and req_email not in _ADMIN_EMAILS:
                            return json.dumps({"status": 403, "err": "Non autorisé à modifier ce message"})
                        m["text"] = nouv_txt
                        if nouv_sent in ("HAUSSIER", "BAISSIER", "NEUTRE"):
                            m["sentiment"] = nouv_sent
                        m["edited"] = datetime.datetime.now().strftime("%d/%m %H:%M")
                        trouve = True
                        break
                if not trouve:
                    return json.dumps({"status": 404, "err": "Message introuvable"})
                _sauver_messages_chat(msgs)
            return json.dumps({"status": 200})

        if cmd == "chat_delete":
            msg_id = str(b.get("id", "")).strip()
            req_email = _email_depuis_token(tk)
            with _CHAT_LOCK:
                msgs = _charger_messages_chat()
                nouv_msgs = []
                supprime = False
                for m in msgs:
                    if str(m.get("id")) == msg_id:
                        owner = str(m.get("owner_email") or "").strip().lower()
                        if owner and req_email != owner and req_email not in _ADMIN_EMAILS:
                            return json.dumps({"status": 403, "err": "Suppression refusée"})
                        supprime = True
                        continue
                    nouv_msgs.append(m)
                if not supprime:
                    return json.dumps({"status": 404, "err": "Message introuvable"})
                _sauver_messages_chat(nouv_msgs)
            return json.dumps({"status": 200})

        if cmd == "chat_list":
            salon = str(b.get("salon", "GENERAL")).strip().upper()
            limit = min(500, max(40, int(b.get("limit", 40))))
            req_email = _email_depuis_token(tk)
            is_admin = req_email in _ADMIN_EMAILS
            all_salon = [m for m in _charger_messages_chat() if m.get("salon", "GENERAL") == salon]
            out_msgs = []
            for m in all_salon[-limit:]:
                mc = dict(m)
                owner = str(mc.pop("owner_email", "") or "").strip().lower()
                mc["can_manage"] = bool(is_admin or (owner and owner == req_email))
                if mc.get("media_id") and mc["media_id"].endswith(".pdf"): mc["pdf_thumb"] = _generer_miniature_pdf_b64(mc["media_id"])
                if not mc.get("preview_rows") and mc.get("media_id") and (mc["media_id"].endswith(".csv") or mc["media_id"].endswith(".xlsx")):
                    mc["preview_rows"] = _extraire_mini_apercu(mc["media_id"])
                out_msgs.append(mc)
            tk_s, repo_s = _hf_data_cfg()
            return json.dumps({
                "status": 200,
                "messages": out_msgs,
                "total": len(all_salon),
                "is_admin": is_admin,
                "sync_info": f"{repo_s or 'NON_CONFIG'} ({_LAST_GH_SYNC_STATUS})"
            })
        if cmd == "chat_set_pseudo":
            nouv_pseudo = re.sub(r"[^a-zA-Z0-9_\-\.]", "", str(b.get("pseudo", "")).strip())[:24]
            if len(nouv_pseudo) < 3:
                return json.dumps({"status": 400, "err": "Pseudo trop court (3 à 24 caractères)"})
            user_info = _TOKENS_ACTIFS.get(tk)
            email_u = user_info.get("email", "") if isinstance(user_info, dict) else (user_info if isinstance(user_info, str) else "")
            if isinstance(user_info, dict):
                user_info["pseudo"] = nouv_pseudo
                user_info["nom"] = nouv_pseudo
            if email_u and "_charger_users_db" in globals() and "_sauver_users_db" in globals():
                try:
                    db_u = _charger_users_db()
                    if email_u in db_u and isinstance(db_u[email_u], dict):
                        db_u[email_u]["pseudo"] = nouv_pseudo
                        _sauver_users_db(db_u)
                except Exception:
                    pass
            return json.dumps({"status": 200, "pseudo": nouv_pseudo})

        if cmd == "chat_post":
            texte = str(b.get("text", "")).strip()
            if not texte or len(texte) > 1000:
                return json.dumps({"status": 400, "err": "Message vide ou trop long (max 1000 caractères)"})
            salon = str(b.get("salon", "GENERAL")).strip().upper()
            sentiment = str(b.get("sentiment", "NEUTRE")).strip().upper()
            quote = str(b.get("quote", "")).strip()[:160]
            ticker_tag = str(b.get("ticker_tag", "")).strip()[:80]
            user_info = _TOKENS_ACTIFS.get(tk)
            auteur = str(b.get("pseudo_client", "")).strip()[:24]
            email_u = ""
            if isinstance(user_info, dict):
                email_u = str(user_info.get("email") or "").strip()
                auteur = str(user_info.get("pseudo") or user_info.get("nom") or user_info.get("username") or auteur).strip()
            elif isinstance(user_info, str):
                email_u = user_info.strip()
            if not auteur and email_u and "_charger_users_db" in globals():
                try:
                    db_u = _charger_users_db()
                    rec = db_u.get(email_u) or {}
                    auteur = str(rec.get("pseudo") or rec.get("nom") or "").strip()
                except Exception:
                    pass
            if not auteur or len(auteur) < 2:
                auteur = email_u.partition("@")[0] if "@" in email_u else (email_u or "Membre")
            with _CHAT_LOCK:
                msgs = _charger_messages_chat()
                nouv = {
                    "id": str(int(time.time() * 1000)),
                    "auteur": auteur,
                    "salon": salon,
                    "sentiment": sentiment,
                    "quote": quote,
                    "ticker_tag": ticker_tag,
                    "owner_email": _email_depuis_token(tk), "media_name": str(b.get("media_name", "")).strip()[:80], "media_id": (_mid_new := _enregistrer_media_direct(b.get("media_data_url", ""), str(b.get("media_name", ""))) or str(b.get("media_id", "")).strip()[:80]), "preview_rows": _extraire_mini_apercu(_mid_new),
                    "text": texte,
                    "heure": datetime.datetime.now().strftime("%d/%m %H:%M")
                }
                msgs.append(nouv)
                _sauver_messages_chat(msgs)
            return json.dumps({"status": 200, "message": nouv})
        if cmd == "index":
            return json.dumps({"status": 200, "data": _construire_index()})
        rel_path = str(b.get("path", "")).lstrip("/")
        # 1. Lecture stricte depuis _BASE_DIR (inclut Historique/...)
        fp = os.path.abspath(os.path.join(_BASE_DIR, rel_path.replace("/", os.sep)))
        if fp.startswith(_BASE_DIR) and os.path.isfile(fp):
            with open(fp, "r", encoding="utf-8", errors="replace") as f:
                return json.dumps({"status": 200, "text": f.read()})
        # 2. Lecture depuis les répertoires temp uniquement pour le mode Live (Cache_Live/ ou temp_...)
        if "Cache_Live/" in rel_path or os.path.basename(rel_path).startswith("temp_"):
            fname = os.path.basename(rel_path)
            for live_dir in _get_live_dirs():
                cand = os.path.join(live_dir, fname)
                if os.path.isfile(cand):
                    with open(cand, "r", encoding="utf-8", errors="replace") as f:
                        return json.dumps({"status": 200, "text": f.read()})
        return json.dumps({"status": 404, "text": ""})
    except Exception as ex:
        return json.dumps({"status": 500, "err": str(ex)})

with gr.Blocks(title="CH-WaveCandle HFT") as demo:
    gr.HTML('<iframe src="/gradio_api/file=CH-WaveCandle.html" style="width:100vw;height:100vh;border:none;position:fixed;inset:0;z-index:9999;"></iframe>')
    gr.Interface(fn=_rpc_register, inputs="text", outputs="text", api_name="bvmt_register", visible=False)
    gr.Interface(fn=_rpc_verify, inputs="text", outputs="text", api_name="bvmt_verify", visible=False)
    gr.Interface(fn=_rpc_login, inputs="text", outputs="text", api_name="bvmt_login", visible=False)
    gr.Interface(fn=_rpc_data, inputs="text", outputs="text", api_name="bvmt_data", visible=False)
    btn_ping = gr.Button("Ping", visible=False)
    btn_ping.click(fn=_hf_gpu_ping, inputs=None, outputs=None)

if __name__ == "__main__":
    threading.Thread(target=lancer_collecteur_bvmt, daemon=True).start()
    threading.Thread(target=boucle_synchro_github, daemon=True).start()
    demo.launch(server_name="0.0.0.0", server_port=int(os.environ.get("PORT", "7860")), ssr_mode=False)

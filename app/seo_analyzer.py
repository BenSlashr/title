import os
import re
import csv
import logging
import requests
import pandas as pd
from typing import List, Tuple, Dict, Set, Optional, Any
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import asyncio
import openai
from openai import AsyncOpenAI
import requests
from bs4 import BeautifulSoup
import re
from typing import Dict, List, Set, Tuple, Any
import time
import random
from collections import defaultdict
from urllib.parse import urlparse
import logging
import asyncio
import aiohttp
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import unicodedata
import numpy as np
from sentence_transformers import SentenceTransformer, util
from sklearn.cluster import AgglomerativeClustering
import nest_asyncio

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Maximum number of concurrent requests
MAX_WORKERS = 20
# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 5

async def scrape_title_async(url: str, session: aiohttp.ClientSession) -> Tuple[str, str]:
    """
    Scrape the title tag from a given URL asynchronously.
    
    Args:
        url: The URL to scrape
        session: aiohttp ClientSession to use for requests
        
    Returns:
        Tuple of (url, title)
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Add timeout to avoid hanging on slow servers
        async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
            if response.status != 200:
                logger.warning(f"Error scraping {url}: HTTP status {response.status}")
                return url, ""
                
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            title_tag = soup.find('title')
            
            if title_tag and title_tag.text:
                return url, title_tag.text.strip()
            return url, ""
    except Exception as e:
        logger.warning(f"Error scraping {url}: {str(e)}")
        return url, ""

def scrape_title(url: str) -> str:
    """
    Scrape the title tag from a given URL (synchronous version for backward compatibility).
    
    Args:
        url: The URL to scrape
        
    Returns:
        The title tag text or an empty string if not found
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        title_tag = soup.find('title')
        
        if title_tag and title_tag.text:
            return title_tag.text.strip()
        return ""
    except Exception as e:
        logger.warning(f"Error scraping {url}: {str(e)}")
        return ""

def scrape_title_with_retry(url: str, max_retries: int = 2) -> str:
    """
    Scrape title with retry logic for resilience.
    
    Args:
        url: URL to scrape
        max_retries: Maximum number of retry attempts
        
    Returns:
        Title or empty string if not found
    """
    retries = 0
    while retries <= max_retries:
        try:
            # Increase timeout for each retry
            timeout = REQUEST_TIMEOUT * (retries + 1)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Add a small random delay to avoid being blocked
            if retries > 0:
                delay = random.uniform(1.0, 3.0)
                time.sleep(delay)
                
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            title_tag = soup.find('title')
            
            if title_tag and title_tag.text:
                return title_tag.text.strip()
            return ""
            
        except requests.exceptions.Timeout:
            retries += 1
            if retries <= max_retries:
                logger.info(f"Timeout for {url}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return ""
                
        except requests.exceptions.HTTPError as e:
            # Don't retry for client errors (4xx)
            if 400 <= e.response.status_code < 500:
                logger.warning(f"Client error {e.response.status_code} for {url}")
                return ""
            
            # Retry for server errors (5xx)
            retries += 1
            if retries <= max_retries:
                logger.info(f"Server error {e.response.status_code} for {url}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return ""
                
        except Exception as e:
            retries += 1
            if retries <= max_retries:
                logger.info(f"Error for {url}: {str(e)}, retrying ({retries}/{max_retries})...")
            else:
                logger.warning(f"Max retries reached for {url}")
                return ""
    
    return ""

def scrape_titles_threaded(urls: List[str], max_workers: int = MAX_WORKERS) -> Dict[str, str]:
    """
    Scrape titles using multi-threading for parallel processing.
    
    Args:
        urls: List of URLs to scrape
        max_workers: Maximum number of threads to use
        
    Returns:
        Dictionary mapping URLs to titles
    """
    results = {}
    total_urls = len(urls)
    
    logger.info(f"Starting threaded scraping of {total_urls} URLs with {max_workers} workers")
    
    # Group URLs by domain to avoid hammering a single domain
    domain_urls = group_urls_by_domain(urls)
    
    # Track problematic domains
    problematic_domains = set()
    
    # Flatten the grouped URLs while respecting domain distribution
    distributed_urls = []
    remaining = True
    
    while remaining:
        remaining = False
        for domain, domain_url_list in list(domain_urls.items()):
            if domain_url_list:
                distributed_urls.append(domain_url_list.pop(0))
                remaining = True
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {executor.submit(scrape_title_with_retry, url): url for url in distributed_urls}
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                title = future.result()
                results[url] = title
                completed += 1
                
                # Log progress every 10 URLs or at the end
                if completed % 10 == 0 or completed == total_urls:
                    logger.info(f"Scraped {completed}/{total_urls} URLs ({(completed/total_urls)*100:.1f}%)")
                
                # If we got a title, the domain is not problematic
                domain = urlparse(url).netloc
                if domain in problematic_domains and title:
                    problematic_domains.remove(domain)
                    
            except Exception as e:
                logger.warning(f"Error scraping {url}: {str(e)}")
                results[url] = ""
                
                # Track problematic domains
                domain = urlparse(url).netloc
                problematic_domains.add(domain)
                
                # If a domain has multiple failures, reduce the number of concurrent requests to it
                if domain in problematic_domains:
                    domain_urls_remaining = [u for u in distributed_urls if urlparse(u).netloc == domain]
                    if domain_urls_remaining:
                        logger.warning(f"Domain {domain} appears problematic. Reducing request rate.")
    
    # Log summary of problematic domains
    if problematic_domains:
        logger.warning(f"Problematic domains with errors: {', '.join(problematic_domains)}")
    
    return results

def extract_keywords(text: str) -> List[str]:
    """
    Extract meaningful keywords from text by removing common stop words.
    
    Args:
        text: The text to extract keywords from
        
    Returns:
        A list of lowercase keywords
    """
    # Simple list of stop words (could be expanded)
    stop_words = {
        'le', 'la', 'les', 'un', 'une', 'des', 'et', 'ou', 'de', 'du', 'en', 'a', 'au', 'aux',
        'par', 'pour', 'avec', 'sans', 'dans', 'sur', 'sous', 'entre', 'vers', 'chez',
        'the', 'a', 'an', 'and', 'or', 'of', 'to', 'in', 'on', 'at', 'by', 'for', 'with',
        'without', 'from', 'as', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
        'this', 'that', 'these', 'those', 'it', 'its', 'it\'s', 'they', 'them', 'their'
    }
    
    # Handle None values
    if text is None:
        return []
    
    # Remove special characters and convert to lowercase
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    
    # Split into words and filter out stop words and short words
    words = [word for word in text.split() if word not in stop_words and len(word) > 2]
    
    # Normalize each word (remove accents, handle plurals)
    normalized_words = [normalize_text(word) for word in words]
    
    return normalized_words

def normalize_text(text: str) -> str:
    """
    Normalize text by removing accents, converting to lowercase, and handling plurals.
    
    Args:
        text: The text to normalize
        
    Returns:
        Normalized text
    """
    if not text:
        return ""
    
    # Convert to lowercase
    text = text.lower()
    
    # Remove accents
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    
    # Handle plurals (basic French and English)
    text = re.sub(r'(s|x|aux)$', '', text)
    
    return text

def calculate_optimization_score(
    title_keywords: List[str], 
    ranking_keywords: List[Tuple[str, int]], 
    title_length: int
) -> int:
    """
    Calculate a simple optimization score based on keyword presence and title length.
    
    Args:
        title_keywords: List of keywords in the title
        ranking_keywords: List of (keyword, impressions) tuples
        title_length: Length of the title in characters
        
    Returns:
        An optimization score from 0-100
    """
    score = 0
    
    # Pré-calculer les versions normalisées des mots-clés du titre (pour éviter les calculs répétés)
    normalized_title_keywords = [normalize_text(kw) for kw in title_keywords]
    normalized_title = normalize_text(" ".join(title_keywords))
    
    # Check if top keywords are in the title (up to 50 points)
    top_keywords = ranking_keywords[:5]  # Consider top 5 keywords
    keywords_in_title = 0
    missing_keywords = []
    
    # Pré-calculer les mots-clés normalisés pour les top keywords (pour éviter les calculs répétés)
    top_keywords_data = []
    for keyword, impressions in top_keywords:
        keyword_words = extract_keywords(keyword)
        normalized_keyword_words = [normalize_text(kw) for kw in keyword_words]
        normalized_keyword = normalize_text(keyword)
        top_keywords_data.append((keyword, impressions, normalized_keyword_words, normalized_keyword))
    
    # Vérifier la présence des mots-clés principaux
    for keyword, _, normalized_keyword_words, normalized_keyword in top_keywords_data:
        # Check if the entire keyword is covered, not just individual words
        keyword_covered = False
        
        # Pour les mots-clés à plusieurs mots, exiger qu'au moins 70% des mots soient présents
        if len(normalized_keyword_words) > 1:
            words_found = sum(1 for kw in normalized_keyword_words if kw in normalized_title_keywords)
            coverage_ratio = words_found / len(normalized_keyword_words) if normalized_keyword_words else 0
            keyword_covered = coverage_ratio >= 0.7
        else:
            # Pour les mots-clés à un seul mot, exiger une correspondance exacte
            keyword_covered = any(kw in normalized_title_keywords for kw in normalized_keyword_words)
        
        # Vérifier si le mot-clé normalisé apparaît comme sous-chaîne dans le titre normalisé
        if not keyword_covered and normalized_keyword in normalized_title:
            keyword_covered = True
            
        if keyword_covered:
            keywords_in_title += 1
        else:
            missing_keywords.append(keyword)
    
    # Calculer le score pour les mots-clés (jusqu'à 50 points)
    if top_keywords:
        score += (keywords_in_title / len(top_keywords)) * 50
    
    # Title length score (up to 30 points)
    if title_length <= 60:
        score += 30
    elif title_length <= 70:
        score += 20
    elif title_length <= 80:
        score += 10
    
    # Keyword density score (up to 20 points)
    if title_keywords and ranking_keywords:
        total_ranking_keywords = sum(impressions for _, impressions in ranking_keywords)
        matched_keywords_impressions = 0
        
        # Utiliser un dictionnaire pour stocker les résultats de normalisation (memoization)
        normalized_keywords_cache = {}
        
        for keyword, impressions in ranking_keywords:
            # Utiliser le cache si le mot-clé a déjà été normalisé
            if keyword in normalized_keywords_cache:
                normalized_kw_words, norm_keyword = normalized_keywords_cache[keyword]
            else:
                keyword_words = extract_keywords(keyword)
                normalized_kw_words = [normalize_text(kw) for kw in keyword_words]
                norm_keyword = normalize_text(keyword)
                normalized_keywords_cache[keyword] = (normalized_kw_words, norm_keyword)
            
            # Vérifier la présence du mot-clé
            keyword_covered = False
            
            # Pour les mots-clés à plusieurs mots
            if len(normalized_kw_words) > 1:
                words_found = sum(1 for kw in normalized_kw_words if kw in normalized_title_keywords)
                coverage_ratio = words_found / len(normalized_kw_words) if normalized_kw_words else 0
                keyword_covered = coverage_ratio >= 0.7
            else:
                keyword_covered = any(kw in normalized_title_keywords for kw in normalized_kw_words)
            
            # Vérifier comme sous-chaîne
            if not keyword_covered and norm_keyword in normalized_title:
                keyword_covered = True
                
            if keyword_covered:
                matched_keywords_impressions += impressions
        
        # Calculer le score de densité
        if total_ranking_keywords > 0:
            keyword_coverage = matched_keywords_impressions / total_ranking_keywords
            score += keyword_coverage * 20
    
    # Calculer le score final
    final_score = min(round(score), 100)
    
    # Logs détaillés (uniquement si nécessaire)
    if missing_keywords:
        logger.info(f"Missing keywords: {', '.join(missing_keywords[:3])}{'...' if len(missing_keywords) > 3 else ''}")
    
    return final_score

def generate_improved_title(
    url: str, 
    original_title: str, 
    ranking_keywords: List[Tuple[str, int]]
) -> str:
    """
    Generate an improved title suggestion based on ranking keywords.
    
    Args:
        url: The URL
        original_title: The original title
        ranking_keywords: List of (keyword, impressions) tuples
        
    Returns:
        A suggested improved title
    """
    # Extraire le nom de domaine pour la marque
    domain_match = re.search(r'://(?:www\.)?([^/]+)', url)
    brand = domain_match.group(1).split('.')[0].capitalize() if domain_match else ""
    
    # Si pas de mots-clés, retourner le titre original
    if not ranking_keywords:
        return original_title
    
    # Extraire les mots-clés et impressions
    keywords = [kw for kw, _ in ranking_keywords]
    impressions = [imp for _, imp in ranking_keywords]
    
    # Calculer le score d'optimisation du titre actuel
    title_keywords = extract_keywords(original_title)
    current_score = calculate_optimization_score(title_keywords, ranking_keywords, len(original_title))
    
    logger.info(f"Applying semantic analysis for {url}")
    
    # Vérifier si l'API OpenAI est configurée
    from dotenv import load_dotenv
    load_dotenv()  # Charger les variables d'environnement depuis le fichier .env
    
    if os.environ.get("OPENAI_API_KEY"):
        try:
            # Utiliser une approche synchrone pour appeler la fonction asynchrone
            nest_asyncio.apply()
            
            # Créer une nouvelle boucle d'événements
            loop = asyncio.get_event_loop()
            gpt_title = loop.run_until_complete(
                generate_title_with_gpt(original_title, ranking_keywords, url, current_score)
            )
            
            logger.info(f"Titre généré par GPT: {gpt_title}")
            return gpt_title
            
        except Exception as e:
            logger.error(f"Erreur lors de la génération du titre avec GPT: {str(e)}")
            logger.info("Fallback à la méthode traditionnelle")
    else:
        logger.info("Clé API OpenAI non configurée, utilisation de la méthode traditionnelle")
    
    # Méthode traditionnelle (fallback)
    try:
        # Créer des groupes thématiques (3 groupes par défaut)
        thematic_groups = create_thematic_groups(keywords, impressions, n_clusters=3)
        
        # Sélectionner le meilleur mot-clé de chaque groupe thématique
        best_keywords = []
        for group_id, group in thematic_groups.items():
            if group:  # S'assurer que le groupe n'est pas vide
                best_keywords.append(group[0][0])  # Prendre le mot-clé avec le plus d'impressions
        
        # Utiliser la fonction create_natural_title pour générer un titre
        return create_natural_title(best_keywords, ranking_keywords, 
                                   domain_match.group(1).split('.')[0].capitalize() if domain_match else "", 
                                   original_title)
                                   
    except Exception as e:
        logger.warning(f"Error during semantic title generation: {str(e)}")
        
        # Fallback simple
        top_keywords = [kw for kw, _ in ranking_keywords[:3]]
        if top_keywords:
            domain_match = re.search(r'://(?:www\.)?([^/]+)', url)
            brand = domain_match.group(1).split('.')[0].capitalize() if domain_match else ""
            
            new_title = f"{top_keywords[0].capitalize()}"
            if len(top_keywords) > 1:
                new_title += f" - {top_keywords[1]}"
            if brand:
                new_title += f" | {brand}"
                
            return new_title
        
        return original_title

async def generate_title_with_gpt(
    original_title: str, 
    keywords: List[Tuple[str, int]], 
    url: str,
    current_score: int
) -> str:
    """
    Génère un titre optimisé en utilisant l'API GPT-4o mini.
    
    Args:
        original_title: Titre original
        keywords: Liste de tuples (mot-clé, impressions)
        url: URL de la page
        current_score: Score d'optimisation actuel
        
    Returns:
        Titre optimisé généré par GPT
    """
    # Extraire le nom de domaine pour la marque
    domain_match = re.search(r'://(?:www\.)?([^/]+)', url)
    brand = domain_match.group(1).split('.')[0].capitalize() if domain_match else ""
    
    # Extraire les mots-clés principaux (jusqu'à 5)
    top_keywords = [kw for kw, _ in keywords[:5]]
    
    # Construire le prompt
    prompt = f"""
    Optimise ce titre SEO pour un site e-commerce français de vente de pompes, cuves et accessoires:
    
    Titre original: {original_title}
    URL: {url}
    Mots-clés principaux (par ordre d'importance): {', '.join(top_keywords)}
    Score d'optimisation actuel: {current_score}/100
    
    Règles:
    - Conserver la structure du titre si elle est déjà bonne
    - Inclure les mots-clés principaux de manière naturelle et fluide
    - Longueur idéale: 50-60 caractères maximum
    - Conserver la marque "{brand}" à la fin après le symbole |
    - Utiliser un langage naturel et grammaticalement correct
    - Si le titre original a un score > 80, ne faire que des améliorations mineures
    - Si le titre est trop long (> 60 caractères), le raccourcir tout en conservant les mots-clés importants
    
    Nouveau titre optimisé:
    """
    
    try:
        # Configuration du client OpenAI
        client = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY", ""))
        
        response = await client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=60
        )
        
        new_title = response.choices[0].message.content.strip()
        
        # Nettoyer le titre (enlever les guillemets ou préfixes éventuels)
        new_title = re.sub(r'^["\']|["\']$|^Nouveau titre optimisé:\s*', '', new_title).strip()
        
        # Vérifier si le titre est vide ou trop court
        if not new_title or len(new_title) < 10:
            return original_title
            
        return new_title
        
    except Exception as e:
        logger.error(f"Erreur lors de l'appel à l'API GPT: {str(e)}")
        return original_title

def create_natural_title(keywords: List[str], ranking_keywords: List[Tuple[str, int]], brand: str, original_title: str = "") -> str:
    """
    Crée un titre naturel en mélangeant intelligemment les mots-clés.
    
    Args:
        keywords: Liste des mots-clés à inclure
        ranking_keywords: Liste complète des mots-clés avec leurs impressions
        brand: Nom de la marque
        original_title: Titre original (pour extraire des éléments utiles)
        
    Returns:
        Un titre naturel combinant les mots-clés
    """
    if not keywords:
        return original_title
    
    # Analyser le titre original pour en extraire des éléments utiles
    original_parts = []
    if original_title:
        # Séparer le titre original en parties (avant et après le séparateur |)
        parts = original_title.split('|')
        if len(parts) > 0:
            original_main = parts[0].strip()
            # Extraire les éléments significatifs du titre original
            original_parts = [p for p in re.split(r'[,\-|]', original_main) if len(p.strip()) > 3]
    
    # Extraire les entités principales (noms communs) des mots-clés
    main_entities = []
    for kw in keywords[:3]:  # Limiter aux 3 premiers mots-clés
        words = extract_keywords(kw)
        # Considérer les premiers mots comme potentiellement des entités principales
        if words:
            main_entities.append(words[0])  # Ajouter le premier mot comme entité principale
            # Ajouter aussi les mots plus longs qui pourraient être des entités
            for word in words:
                if len(word) > 4 and word not in main_entities:  # Mots plus longs sont probablement significatifs
                    main_entities.append(word)
    
    # Extraire les qualificatifs (adjectifs, compléments) des mots-clés
    qualifiers = []
    for kw in keywords[:3]:
        # Trouver les expressions comme "à eau", "de pluie", "pour pompe", etc.
        matches = re.findall(r'(?:à|de|pour|en|avec)\s+\w+(?:\s+\w+)?', kw)
        for match in matches:
            if match not in qualifiers:
                qualifiers.append(match)
    
    # Extraire les types de produits (citerne, récupérateur, compteur, etc.)
    product_types = []
    common_product_types = ["pompe", "cuve", "compteur", "citerne", "récupérateur", "crépine", "clapet", "filtre", "réservoir"]
    
    # D'abord, chercher dans les mots-clés principaux
    for kw in keywords[:5]:
        for word in extract_keywords(kw):
            if word.lower() in common_product_types and word not in product_types:
                product_types.append(word)
    
    # Si aucun type de produit n'est trouvé, chercher dans tous les mots-clés
    if not product_types:
        for kw, _ in ranking_keywords[:10]:
            for word in extract_keywords(kw):
                if word.lower() in common_product_types and word not in product_types:
                    product_types.append(word)
    
    # Extraire les adjectifs qualificatifs (électrique, professionnel, etc.)
    adjectives = []
    common_adjectives = ["électrique", "professionnel", "professionnelle", "automatique", "manuel", "digital"]
    
    for kw in keywords[:5]:
        for adj in common_adjectives:
            if adj in kw.lower() and adj not in adjectives:
                adjectives.append(adj)
    
    # Extraire les caractéristiques spécifiques (gasoil, diesel, GNR, etc.)
    specifics = []
    specific_terms = ["gasoil", "diesel", "gnr", "fuel", "eau", "huile"]
    for kw in keywords[:5]:
        for word in extract_keywords(kw):
            if word.lower() in specific_terms and word not in specifics:
                specifics.append(word)
    
    # Vérifier si le titre original contient déjà des éléments importants
    has_good_structure = False
    if original_title:
        original_lower = original_title.lower()
        # Vérifier si le titre original contient déjà des types de produits
        product_in_original = any(pt.lower() in original_lower for pt in product_types)
        # Vérifier si le titre original contient des spécificités
        specifics_in_original = any(sp.lower() in original_lower for sp in specifics)
        # Vérifier si le titre original contient des qualificatifs
        qualifiers_in_original = any(q.lower() in original_lower for q in qualifiers)
        
        # Si le titre original contient déjà beaucoup d'éléments importants, le conserver comme base
        if product_in_original and (specifics_in_original or qualifiers_in_original):
            has_good_structure = True
    
    # Construire le titre en fonction de l'analyse
    if has_good_structure:
        # Partir du titre original mais l'enrichir si nécessaire
        main_part = original_title.split('|')[0].strip()
        
        # Ajouter des éléments manquants importants
        missing_specifics = [sp for sp in specifics if sp.lower() not in main_part.lower()]
        if missing_specifics and len(main_part) + len(missing_specifics[0]) + 5 <= 50:
            main_part = f"{main_part} {missing_specifics[0]}"
        
        # Reconstruire le titre avec la marque
        if brand and '|' in original_title:
            title = f"{main_part} | {brand}"
        else:
            title = main_part
    else:
        # Construire un nouveau titre à partir des éléments extraits
        title_parts = []
        
        # Commencer par les types de produits
        if product_types:
            product_type = product_types[0].capitalize()
            title_parts.append(product_type)
        elif main_entities:
            main_entity = main_entities[0].capitalize()
            title_parts.append(main_entity)
        
        # Ajouter les qualificatifs
        if qualifiers:
            for qualifier in qualifiers:
                if not any(qualifier.lower() in part.lower() for part in title_parts):
                    title_parts.append(qualifier)
        
        # Ajouter les adjectifs
        if adjectives:
            for adj in adjectives:
                if not any(adj.lower() in part.lower() for part in title_parts):
                    title_parts.append(adj)
        
        # Ajouter les spécificités
        if specifics:
            specifics_str = " ".join(specifics)
            if not any(sp.lower() in " ".join(title_parts).lower() for sp in specifics):
                title_parts.append(specifics_str)
        
        # Construire le titre principal
        title = " ".join(title_parts)
        
        # Si le titre est trop court, ajouter d'autres mots-clés importants
        if len(title) < 30 and keywords:
            for kw in keywords[:2]:
                if kw.lower() not in title.lower() and len(title) + len(kw) + 3 <= 50:
                    title = f"{title} - {kw}"
                    break
        
        # Ajouter la marque
        if brand:
            title = f"{title} | {brand}"
    
    # Nettoyer les espaces multiples et autres problèmes de formatage
    title = re.sub(r'\s+', ' ', title).strip()
    title = re.sub(r',\s*,', ',', title)
    title = re.sub(r'\s*\|\s*', ' | ', title)
    
    # Vérifier si le titre est trop générique ou trop court
    if len(title.split()) <= 3 and original_title and len(original_title.split()) > 3:
        # Le titre généré est trop court/générique, revenir au titre original
        return original_title
    
    return title

def normalize_column_name(name: str) -> str:
    """
    Normalize column names by removing spaces, special characters, and converting to lowercase.
    
    Args:
        name: The original column name
        
    Returns:
        Normalized column name
    """
    # Handle None values
    if name is None:
        return ""
    
    # Remove special characters, convert to lowercase, and replace spaces with underscores
    normalized = re.sub(r'[^\w\s]', '', name.lower()).strip().replace(' ', '_')
    return normalized

def get_column_mapping(csv_data: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Create a mapping between expected column names and actual column names in the CSV.
    
    Args:
        csv_data: List of dictionaries containing CSV data
        
    Returns:
        Dictionary mapping expected column names to actual column names
    """
    if not csv_data:
        return {}
    
    # Get the first row to examine column names
    first_row = csv_data[0]
    
    # Define expected column names and their possible variations
    expected_columns = {
        'url': ['url', 'page', 'landing page', 'landing_page', 'page url', 'page_url', 'top pages', 'top_pages'],
        'query': ['query', 'keyword', 'search term', 'search_term', 'search query', 'search_query', 'queries', 'top queries'],
        'impressions': ['impressions', 'impr', 'impression', 'views', 'imprs'],
        'clicks': ['clicks', 'click', 'clics', 'clic'],
        'ctr': ['ctr', 'click through rate', 'click_through_rate', 'clickthrough', 'click-through rate'],
        'position': ['position', 'pos', 'ranking', 'rank', 'average position', 'avg position', 'avg_position']
    }
    
    # Create mapping
    column_mapping = {}
    
    # Print available columns for debugging
    logger.info(f"Available columns in CSV: {list(first_row.keys())}")
    
    # Try direct matching first
    for expected_col, variations in expected_columns.items():
        for col in first_row.keys():
            if col in variations or normalize_column_name(col) in [normalize_column_name(v) for v in variations]:
                column_mapping[expected_col] = col
                break
    
    # If direct matching fails, try fuzzy matching with normalized column names
    if len(column_mapping) < 3:  # We need at least url, query, and impressions
        normalized_columns = {normalize_column_name(col): col for col in first_row.keys() if col is not None}
        
        for expected_col, variations in expected_columns.items():
            if expected_col not in column_mapping:  # Only check if not already mapped
                for variation in variations:
                    normalized_variation = normalize_column_name(variation)
                    for norm_col, orig_col in normalized_columns.items():
                        if normalized_variation in norm_col or norm_col in normalized_variation:
                            column_mapping[expected_col] = orig_col
                            break
                    if expected_col in column_mapping:
                        break
    
    # Print the mapping for debugging
    logger.info(f"Column mapping: {column_mapping}")
    
    return column_mapping

def group_urls_by_domain(urls: List[str], max_per_domain: int = 5) -> Dict[str, List[str]]:
    """
    Group URLs by domain to avoid hammering a single domain with too many requests.
    
    Args:
        urls: List of URLs to group
        max_per_domain: Maximum number of URLs per domain in each group
        
    Returns:
        Dictionary mapping domain to list of URLs
    """
    domain_urls = defaultdict(list)
    
    for url in urls:
        try:
            parsed_url = urlparse(url)
            domain = parsed_url.netloc
            domain_urls[domain].append(url)
        except Exception:
            continue
    
    return domain_urls

def chunk_urls(urls: List[str], chunk_size: int = 50) -> List[List[str]]:
    """
    Split URLs into chunks for batch processing.
    
    Args:
        urls: List of URLs to chunk
        chunk_size: Maximum size of each chunk
        
    Returns:
        List of URL chunks
    """
    return [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

def analyze_csv(csv_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Analyze the CSV data from Google Search Console.
    
    Args:
        csv_data: List of dictionaries containing GSC data
        
    Returns:
        List of dictionaries with analysis results
    """
    if not csv_data:
        raise ValueError("CSV data is empty")
    
    # Get column mapping
    column_mapping = get_column_mapping(csv_data)
    
    # Check if required columns are present
    required_columns = ['url', 'query', 'impressions']
    missing_columns = [col for col in required_columns if col not in column_mapping]
    
    if missing_columns:
        missing_cols_str = ', '.join(missing_columns)
        raise ValueError(f"Required column(s) not found in CSV: {missing_cols_str}. Available columns: {', '.join(csv_data[0].keys())}")
    
    # Group by URL and aggregate data
    url_data = defaultdict(list)
    logger.info(f"Processing {len(csv_data)} rows from CSV")
    
    for row in csv_data:
        # Get values using the mapped column names
        url_raw = row[column_mapping['url']]
        query = row[column_mapping['query']]
        
        # Clean URL if it contains semicolons (which might include the query)
        url = url_raw.split(';')[0] if ';' in url_raw else url_raw
        
        # Convert impressions to integer
        try:
            impressions_str = row[column_mapping['impressions']]
            # If impressions is part of a combined string with semicolons, extract it
            if ';' in impressions_str and not impressions_str.isdigit():
                parts = url_raw.split(';')
                if len(parts) >= 3:  # URL;Query;Impressions;...
                    impressions = int(float(parts[2].strip()))
                else:
                    impressions = 0
            else:
                impressions = int(float(impressions_str))
        except (ValueError, TypeError, IndexError):
            impressions = 0
        
        url_data[url].append((query, impressions))
    
    # Filter out invalid URLs
    valid_urls = [url for url in url_data.keys() if url and url.startswith(('http://', 'https://'))]
    logger.info(f"Found {len(valid_urls)} unique valid URLs to analyze")
    
    # Use threaded scraping for titles
    logger.info("Starting threaded title scraping process")
    title_data = scrape_titles_threaded(valid_urls)
    
    # Process each URL
    results = []
    logger.info("Analyzing titles and keywords")
    
    for url in valid_urls:
        # Get title from scraped data
        title = title_data.get(url, "")
        
        # Get keywords for this URL
        keywords = url_data[url]
        
        # Sort keywords by impressions
        sorted_keywords = sorted(keywords, key=lambda x: x[1], reverse=True)
        
        # Extract keywords from title
        title_keywords = extract_keywords(title)
        
        # Check if main keywords are in title
        missing_keywords = []
        for keyword, impressions in sorted_keywords[:5]:  # Check top 5 keywords
            keyword_words = extract_keywords(keyword)
            if not any(kw in title_keywords for kw in keyword_words):
                missing_keywords.append(keyword)
        
        # Calculate optimization score
        score = calculate_optimization_score(title_keywords, sorted_keywords, len(title))
        
        # Generate improved title suggestion
        improved_title = generate_improved_title(url, title, sorted_keywords)
        
        # Create result entry
        result = {
            'url': url,
            'current_title': title,
            'title_length': len(title),
            'title_too_long': len(title) > 60,
            'top_keywords': [kw for kw, _ in sorted_keywords[:5]],
            'missing_keywords': missing_keywords,
            'optimization_score': score,
            'suggested_title': improved_title
        }
        
        results.append(result)
    
    logger.info(f"Analysis complete. Generated results for {len(results)} URLs")
    return results

def generate_report(analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate a summary report from the analysis results.
    
    Args:
        analysis_results: List of dictionaries with analysis results
        
    Returns:
        Dictionary with report summary
    """
    total_urls = len(analysis_results)
    urls_with_long_titles = sum(1 for result in analysis_results if result['title_too_long'])
    urls_missing_keywords = sum(1 for result in analysis_results if len(result['missing_keywords']) > 0)
    
    if total_urls > 0:
        avg_score = sum(result['optimization_score'] for result in analysis_results) / total_urls
    else:
        avg_score = 0
    
    return {
        'total_urls': total_urls,
        'urls_with_long_titles': urls_with_long_titles,
        'urls_missing_keywords': urls_missing_keywords,
        'average_optimization_score': round(avg_score, 1)
    }

def find_semantic_clusters(keywords: List[str], threshold: float = 0.75) -> List[List[str]]:
    """
    Regroupe les mots-clés en clusters sémantiques en utilisant Sentence Transformers
    
    Args:
        keywords: Liste de mots-clés à regrouper
        threshold: Seuil de similarité (entre 0 et 1)
        
    Returns:
        Liste de clusters (chaque cluster étant une liste de mots-clés)
    """
    if not keywords:
        return []
    
    # Obtenir le modèle
    model = get_sentence_model()
    
    # Encoder tous les mots-clés
    embeddings = model.encode(keywords)
    
    # Calculer la matrice de similarité
    similarity_matrix = util.pytorch_cos_sim(embeddings, embeddings).numpy()
    
    # Identifier les clusters
    clusters = []
    used_keywords = set()
    
    for i, keyword in enumerate(keywords):
        if keyword in used_keywords:
            continue
            
        cluster = [keyword]
        used_keywords.add(keyword)
        
        # Trouver les mots-clés similaires
        for j, similar_keyword in enumerate(keywords):
            if i != j and similar_keyword not in used_keywords:
                if similarity_matrix[i][j] > threshold:
                    cluster.append(similar_keyword)
                    used_keywords.add(similar_keyword)
        
        clusters.append(cluster)
    
    return clusters

def create_thematic_groups(keywords: List[str], impressions: List[int], n_clusters: int = 3) -> Dict[int, List[Tuple[str, int]]]:
    """
    Crée des groupes thématiques à partir des mots-clés en utilisant le clustering
    
    Args:
        keywords: Liste de mots-clés
        impressions: Liste d'impressions correspondant aux mots-clés
        n_clusters: Nombre de clusters à créer
        
    Returns:
        Dictionnaire de groupes thématiques avec les mots-clés et leurs impressions
    """
    if not keywords or len(keywords) < n_clusters:
        return {0: [(kw, imp) for kw, imp in zip(keywords, impressions)]}
    
    # Obtenir le modèle
    model = get_sentence_model()
    
    # Encoder les mots-clés
    embeddings = model.encode(keywords)
    
    # Appliquer le clustering hiérarchique
    clustering = AgglomerativeClustering(n_clusters=min(n_clusters, len(keywords)))
    cluster_labels = clustering.fit_predict(embeddings)
    
    # Organiser les résultats
    thematic_groups = {}
    for i, label in enumerate(cluster_labels):
        if label not in thematic_groups:
            thematic_groups[label] = []
        thematic_groups[label].append((keywords[i], impressions[i]))
    
    # Trier chaque groupe par nombre d'impressions
    for label in thematic_groups:
        thematic_groups[label].sort(key=lambda x: x[1], reverse=True)
    
    return thematic_groups

_model = None

def get_sentence_model():
    """
    Get the sentence transformer model with lazy loading
    """
    global _model
    if _model is None:
        logger.info("Loading sentence transformer model...")
        _model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')
        logger.info("Model loaded successfully")
    return _model
